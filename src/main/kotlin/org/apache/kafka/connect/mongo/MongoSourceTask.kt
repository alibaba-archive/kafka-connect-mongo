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
import org.apache.kafka.connect.mongo.tools.JmxTool
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory

import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

interface MongoSourceTaskMBean {
    var mSleepTime: Long
    val mOffsets: HashMap<Map<String, String>, Map<String, Any>>
    var mRecordCount: Int
}

/**
 * Created by Xu Jingxin on 16/8/3.
 */
class MongoSourceTask : SourceTask(), MongoSourceTaskMBean {
    private val log = LoggerFactory.getLogger(MongoSourceTask::class.java)

    private var uri = ""
    private var schemaName: String? = null
    private var batchSize = 100
    private var topicPrefix: String? = null
    private var databases = listOf<String>()

    private val offsets = HashMap<Map<String, String>, Map<String, Any>>()
    // Sleep time will get double of it's self when there was no records return in the poll function
    // But will not larger than maxSleepTime
    private var sleepTime = 50L
    private var maxSleepTime = 10000L
    // How many times will a process retries before quit
    private val maxErrCount = 5
    internal var messages = ConcurrentLinkedQueue<Document>()

    override var mSleepTime: Long = sleepTime
        get() = sleepTime
    override val mOffsets: HashMap<Map<String, String>, Map<String, Any>>
        get() = offsets
    override var mRecordCount = 0

    init {
        JmxTool.registerMBean(this)
    }

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
        uri = props[MONGO_URI_CONFIG] as String
        databases = Arrays.asList<String>(*props[DATABASES_CONFIG]!!.split(",".toRegex()).dropLastWhile(String::isEmpty).toTypedArray())

        log.trace("Creating schema")

        databases.map { it.replace("[\\s.]".toRegex(), "_") }
                .forEach {
                    schemas.put(it, SchemaBuilder.struct().name(schemaName + "_" + it)
                            .field("ts", Schema.OPTIONAL_INT32_SCHEMA).field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                            .field("id", Schema.OPTIONAL_STRING_SCHEMA).field("database", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("object", Schema.OPTIONAL_STRING_SCHEMA).build())
                }

        loadOffsets()
        for (database in databases) {
            startDBReader(database, 0)
        }
    }

    @Throws(InterruptedException::class)
    override fun poll(): List<SourceRecord> {
        log.trace("Polling records")
        val records = mutableListOf<SourceRecord>()
        while (!messages.isEmpty() && records.size < batchSize) {
            val message = messages.poll()
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
            sleepTime = Math.min(sleepTime * 2, maxSleepTime)
            Thread.sleep(sleepTime)
        } else {
            sleepTime = 50L
        }
        mRecordCount += records.count()
        return records
    }

    override fun stop() {

    }

    // Create a new DatabaseReader thread for
    @Throws(Exception::class)
    private fun startDBReader(db: String, errCount: Int = 0) {
        if (errCount > maxErrCount) {
            throw Exception("Can not execute database reader task!")
        }
        val startTime = System.currentTimeMillis()
        var start = "0,0"
        val timeOffset = offsets[MongoSourceTask.getPartition(db)]
        if (!(timeOffset == null || timeOffset.isEmpty())) start = timeOffset[db] as String
        log.trace("Starting database reader with configuration: ")
        log.trace("uri: {}", uri)
        log.trace("db: {}", db)
        log.trace("start: {}", timeOffset)
        val uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { thread, throwable ->
            throwable.printStackTrace()
            log.error("Error when read data from db: {}", db)
            var _errCount = errCount + 1
            // Reset error count if the task executed more than 5 minutes
            if ((System.currentTimeMillis() - startTime) > 600000) {
               _errCount = 0
            }
            loadOffsets()
            startDBReader(db, _errCount)
        }
        val reader = DatabaseReader(uri, db, start, messages)
        JmxTool.registerMBean(reader)
        val t = Thread(reader)
        t.uncaughtExceptionHandler = uncaughtExceptionHandler
        t.start()
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
        val partitions = databases.map({ MongoSourceTask.getPartition(it) })
        offsets.putAll(context.offsetStorageReader().offsets<String>(partitions))
    }

    companion object {
        private var schemas: MutableMap<String, Schema> = HashMap()

        fun getPartition(db: String): Map<String, String> {
            return Collections.singletonMap("mongo", db)
        }
    }
}
