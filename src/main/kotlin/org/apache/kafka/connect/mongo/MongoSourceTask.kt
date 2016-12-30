package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.BATCH_SIZE_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.DATABASES_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.MONGO_URI_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.SCHEMA_NAME_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.TOPIC_PREFIX_CONFIG
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

    private var uri: String? = null
    private var schemaName: String? = null
    private var batchSize = 100
    private var topicPrefix: String? = null
    private var databases: List<String>? = null

    private var reader: MongoReader? = null
    private val offsets = HashMap<Map<String, String>, Map<String, Any>>()
    // Sleep time will get double of it's self when there was no records return in the poll function
    // But will not larger than maxSleepTime
    private var sleepTime = 50
    private var maxSleepTime = 10000

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
            batchSize = Integer.parseInt(props[BATCH_SIZE_CONFIG])
        } catch (e: Exception) {
            throw ConnectException(BATCH_SIZE_CONFIG + " config should be an Integer")
        }

        schemaName = props[SCHEMA_NAME_CONFIG]
        topicPrefix = props[TOPIC_PREFIX_CONFIG]
        uri = props[MONGO_URI_CONFIG]
        databases = Arrays.asList<String>(*props[DATABASES_CONFIG]!!.split(",".toRegex()).dropLastWhile(String::isEmpty).toTypedArray())

        log.trace("Creating schema")

        databases!!.map { it.replace("[\\s.]".toRegex(), "_") }
                .forEach {
                    schemas.put(it, SchemaBuilder.struct().name(schemaName + "_" + it)
                            .field("ts", Schema.OPTIONAL_INT32_SCHEMA).field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                            .field("id", Schema.OPTIONAL_STRING_SCHEMA).field("database", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("object", Schema.OPTIONAL_STRING_SCHEMA).build())
                }

        loadOffsets()
        reader = MongoReader(uri!!, databases!!, offsets)
        reader!!.run()
    }

    @Throws(InterruptedException::class)
    override fun poll(): List<SourceRecord> {
        log.trace("Polling records")
        val records = mutableListOf<SourceRecord>()
        while (!reader!!.messages.isEmpty() && records.size < batchSize) {
            val message = reader!!.messages.poll()
            val struct = getStruct(message)
            records.add(SourceRecord(
                    getPartition(getDB(message)),
                    getOffset(message),
                    getTopic(message),
                    Schema.OPTIONAL_STRING_SCHEMA,
                    struct.get("id"),
                    struct.schema(),
                    struct))
            log.trace(message.toString())
        }
        if (records.size == 0) {
            sleepTime *= 2
            Thread.sleep(Math.min(sleepTime, maxSleepTime).toLong())
        } else {
            sleepTime = 50
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
        val schema = schemas[db]
        val struct = Struct(schema)
        val bsonTimestamp = message["ts"] as BsonTimestamp
        val body = message["o"] as Document
        val _id = (body["_id"] as ObjectId).toString()
        struct.put("ts", bsonTimestamp.time)
        struct.put("inc", bsonTimestamp.inc)
        struct.put("id", _id)
        struct.put("database", db)
        if (message["op"].toString() != "d") {
            struct.put("object", (message["o"] as Document).toJson())
        }
        return struct
    }

    private fun loadOffsets() {
        val partitions = databases!!.map({ MongoSourceTask.getPartition(it) })
        offsets.putAll(context.offsetStorageReader().offsets<String>(partitions))
    }

    companion object {
        private var schemas: MutableMap<String, Schema> = HashMap()

        fun getPartition(db: String): Map<String, String> {
            return Collections.singletonMap("mongo", db)
        }
    }
}
