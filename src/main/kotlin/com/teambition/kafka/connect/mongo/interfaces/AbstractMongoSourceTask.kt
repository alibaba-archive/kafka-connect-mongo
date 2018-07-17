package com.teambition.kafka.connect.mongo.interfaces

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.BATCH_SIZE_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.DATABASES_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.INITIAL_IMPORT_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.MONGO_URI_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.SCHEMA_NAME_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.TOPIC_PREFIX_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConnector
import com.teambition.kafka.connect.mongo.MongoSourceOffset
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.Logger
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
abstract class AbstractMongoSourceTask : SourceTask() {
    abstract val log: Logger
    // Configs
    protected var uri = ""
    private var schemaName = ""
    private var batchSize = 100
    protected var initialImport = false
    private var topicPrefix = ""
    // Database and collection joined with dot [mydb.a,mydb.b]
    protected var databases = listOf<String>()
    private var schemas = mutableMapOf<String, Schema>()
    // Message queue
    protected val messages = ConcurrentLinkedQueue<Document>()
    // Runtime states
    // Sleep time will get double of it's self when there was no records return in the poll function
    // But will not larger than maxSleepTime
    private var sleepTime = 50L
    private var maxSleepTime = 10000L

    override fun version(): String = MongoSourceConnector().version()
    protected var unrecoverable: Throwable? = null

    /**
     * Parse the config properties into in-use type and format
     * @param props
     */
    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration: {}", props)
        batchSize = Integer.parseInt(props[BATCH_SIZE_CONFIG])
        initialImport = props[INITIAL_IMPORT_CONFIG]?.toLowerCase()?.equals("true") ?: false
        schemaName = props[SCHEMA_NAME_CONFIG] ?: throw Exception("Invalid config $SCHEMA_NAME_CONFIG")
        topicPrefix = props[TOPIC_PREFIX_CONFIG] ?: throw Exception("Invalid config $TOPIC_PREFIX_CONFIG")
        uri = props[MONGO_URI_CONFIG] ?: throw Exception("Invalid config $MONGO_URI_CONFIG")
        databases = props[DATABASES_CONFIG]!!.split(",").map(String::trim).dropLastWhile(String::isEmpty)

        log.trace("Init schema")
        databases.map { it.replace(".", "_") }
            .forEach {
                schemas[it] = SchemaBuilder.struct().name(schemaName + "_" + it)
                    .field("ts", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("op", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("object", Schema.OPTIONAL_STRING_SCHEMA).build()
            }
    }

    override fun poll(): List<SourceRecord> {
        unrecoverable?.let { throw it }
        log.trace("Polling records")
        val records = mutableListOf<SourceRecord>()
        while (!messages.isEmpty() && records.size < batchSize) {
            val message = messages.poll()
            try {
                val struct = getStruct(message)
                records.add(SourceRecord(
                    getPartition(getDB(message)),
                    getOffset(message),
                    getTopic(message),
                    Schema.OPTIONAL_STRING_SCHEMA,
                    struct.get("id"),
                    struct.schema(),
                    struct))
            } catch (e: Exception) {
                log.error(e.message)
            }
            log.trace(message.toString())
        }
        if (records.size == 0) {
            sleepTime = Math.min(sleepTime * 2, maxSleepTime)
            Thread.sleep(sleepTime)
        } else {
            sleepTime = 50L
        }
        return records
    }

    override fun stop() {
    }

    /**
     * @param db database with collection like 'mydb.test'
     */
    protected fun getPartition(db: String): Map<String, String> {
        return Collections.singletonMap("mongo", db)
    }

    private fun getOffset(message: Document): Map<String, String> {
        val timestamp = message["ts"] as BsonTimestamp
        val objectId = (message["o"] as Document)["_id"] as ObjectId
        val finishedImport = message["initialImport"] == null
        val offsetVal = MongoSourceOffset.toOffsetString(timestamp, objectId, finishedImport)
        return Collections.singletonMap(getDB(message), offsetVal)
    }

    private fun getDB(message: Document): String {
        return message["ns"] as String
    }

    private fun getTopic(message: Document): String {
        val db = getDB(message).replace(".", "_")
        return topicPrefix + "_" + db
    }

    private fun getStruct(message: Document): Struct {
        val db = getDB(message).replace(".", "_")
        val schema = schemas[db] ?: throw Exception("Can not find the schema of database $db")
        val struct = Struct(schema)
        val bsonTimestamp = message["ts"] as BsonTimestamp
        val body = message["o"] as Document
        val _id = (body["_id"] as ObjectId).toString()
        struct.put("ts", bsonTimestamp.time)
        struct.put("inc", bsonTimestamp.inc)
        struct.put("id", _id)
        struct.put("database", db)
        struct.put("op", message["op"])
        if (message["op"].toString() == "d") {
            struct.put("object", null)
        } else {
            struct.put("object", (message["o"] as Document).toJson())
        }
        return struct
    }
}
