package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory

import java.util.*

/**
 * Created by Xu Jingxin on 16/8/3.
 */
class MongoSourceTask : SourceTask() {
    private val log = LoggerFactory.getLogger(MongoSourceTask::class.java)

    private var port: Int? = null
    private var host: String? = null
    private var schemaName: String? = null
    private var batchSize: Int? = null
    private var topicPrefix: String? = null
    private var databases: List<String>? = null

    private var reader: MongoReader? = null
    private val offsets = HashMap<Map<String, String>, Map<String, Any>>()

    override fun version(): String {
        return MongoSourceConnector().version()
    }

    /**
     * Parse the config properties into in-use type and format
     * @param props
     */
    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration")

        try {
            port = Integer.parseInt(props[MongoSourceConfig.PORT_CONFIG])
        } catch (e: Exception) {
            throw ConnectException(MongoSourceConfig.PORT_CONFIG + " config should be an Integer")
        }

        try {
            batchSize = Integer.parseInt(props[MongoSourceConfig.BATCH_SIZE_CONFIG])
        } catch (e: Exception) {
            throw ConnectException(MongoSourceConfig.BATCH_SIZE_CONFIG + " config should be an Integer")
        }

        schemaName = props[MongoSourceConfig.SCHEMA_NAME_CONFIG]
        topicPrefix = props[MongoSourceConfig.SCHEMA_NAME_CONFIG]
        host = props[MongoSourceConfig.HOST_CONFIG]
        databases = Arrays.asList<String>(*props[MongoSourceConfig.DATABASES_CONFIG]!!.split(",".toRegex()).dropLastWhile(String::isEmpty).toTypedArray())

        log.trace("Creating schema")
        if (schemas == null) schemas = HashMap<String, Schema>()

        for (database in databases!!) {
            val db = database.replace("[\\s.]".toRegex(), "_")
            (schemas as MutableMap<String, Schema>)
                    .put(db, SchemaBuilder.struct().name(schemaName + "_" + db)
                    .field("ts", Schema.OPTIONAL_INT32_SCHEMA).field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("id", Schema.OPTIONAL_STRING_SCHEMA).field("database", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("object", Schema.OPTIONAL_STRING_SCHEMA).build())
        }

        loadOffsets()
        reader = MongoReader(host!!, port!!, databases!!, offsets)
        reader!!.run()
    }

    @Throws(InterruptedException::class)
    override fun poll(): List<SourceRecord> {
        val records = mutableListOf<SourceRecord>()
        while (!reader!!.messages.isEmpty() && records.size < batchSize!!) {
            val message = reader!!.messages.poll()
            val messageStruct = getStruct(message)
            records.add(SourceRecord(
                    getPartition(getDB(message)),
                    getOffset(message),
                    getTopic(message),
                    getStruct(message).schema(),
                    messageStruct))
            log.trace(message.toString())
        }
        return records
    }

    override fun stop() {

    }

    private fun getOffset(message: Document): Map<String, String> {
        val timestamp = message["ts"] as BsonTimestamp
        val offsetVal = timestamp.time.toString() + "," + timestamp.inc
        return Collections.singletonMap(getDB(message), offsetVal)
    }

    private fun getDB(message: Document): String {
        return message["ns"] as String
    }

    private fun getTopic(message: Document): String {
        var db = getDB(message)
        db = db.replace("[\\s.]".toRegex(), "_")
        if (topicPrefix != null && !topicPrefix!!.isEmpty()) {
            return topicPrefix + "_" + db
        }
        return db
    }

    private fun getStruct(message: Document): Struct {
        val db = getDB(message).replace("[\\s.]".toRegex(), "_")
        val schema = schemas!![db]
        val messageStruct = Struct(schema)
        val bsonTimestamp = message["ts"] as BsonTimestamp
        val body = message["o"] as Document
        val _id = (body.get("_id") as ObjectId).toString()
        messageStruct.put("ts", bsonTimestamp.time)
        messageStruct.put("inc", bsonTimestamp.inc)
        messageStruct.put("id", _id)
        messageStruct.put("database", db)
        if (message["op"].toString() != "d") {
            messageStruct.put("object", (message["o"] as Document).toJson())
        }
        return messageStruct
    }

    private fun loadOffsets() {
        val partitions = databases!!.map({ MongoSourceTask.getPartition(it) })
        offsets.putAll(context.offsetStorageReader().offsets<String>(partitions))
    }

    companion object {
        private var schemas: MutableMap<String, Schema>? = null

        fun getPartition(db: String): Map<String, String> {
            return Collections.singletonMap("mongo", db)
        }
    }
}
