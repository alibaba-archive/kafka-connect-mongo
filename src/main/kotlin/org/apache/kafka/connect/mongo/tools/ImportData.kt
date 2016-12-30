package org.apache.kafka.connect.mongo.tools

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.connect.mongo.MongoSourceConfig
import org.bson.types.ObjectId
import org.json.JSONObject
import java.io.FileInputStream
import java.util.*

/**
 * @author Xu Jingxin
 * Import all the data from one collection into kafka
 * Then save the offset *
 * @param uri mongodb://[user:pwd@]host:port
 * @param dbs Database.collection strings, combined with comma
 * @param topicPrefix Producer topic in kafka
 * @param props Kafka producer props
 */
class ImportJob(val uri: String,
                val dbs: String,
                val topicPrefix: String,
                val props: Properties,
                val bulkSize: Int = 1000) {

    companion object {
        private val log = LoggerFactory.getLogger(ImportJob::class.java)
    }
    private val messages = ConcurrentLinkedQueue<JSONObject>()
    private var producer: KafkaProducer<String, String>

    init {
       producer = KafkaProducer(props)
    }

    /**
     * Start job
     */
    fun start() {
        log.info("Start import data from {}", dbs)
        val threadGroup = mutableListOf<Thread>()
        var threadCount = 0
        dbs.split(",").dropLastWhile(String::isEmpty).forEach {
            log.trace("Import database: {}", it)
            val importDB = ImportDB(uri, it, messages, bulkSize)
            val t = Thread(importDB)
            threadCount += 1
            threadGroup.add(t)
            t.start()
        }

        while (true) {
            threadCount = threadGroup.filter(Thread::isAlive).count()
            if (threadCount == 0 && messages.isEmpty()) {
                break
            }
            flush()
            Thread.sleep(100)
        }

        producer.close()
        log.info("Import finish")
    }

    /**
     * Flush messages into kafka
     */
    fun flush() {
        while (!messages.isEmpty()) {
            val message = messages.poll()
            log.trace("Poll message {}", message)
            val db = message["database"] as String
            val topic = "${topicPrefix}_$db"

            val record = ProducerRecord<String, String>(
                    topic,
                    message["id"].toString(),
                    message.toString())
            log.trace("Record {}", record)
            producer.send(record)
        }
    }

}

/**
 * Import data from single collection
 * @param uri mongodb://[user:pwd@]host:port
 * @param dbName mydb.users
 */
class ImportDB(val uri: String,
               val dbName: String,
               var messages: ConcurrentLinkedQueue<JSONObject>,
               val bulkSize: Int) : Runnable {

    private val mongoClient: MongoClient
    private val mongoDatabase: MongoDatabase
    private val mongoCollection: MongoCollection<Document>
    private var skipOffset = 0

    companion object {
        private val log = LoggerFactory.getLogger(ImportDB::class.java)
    }

    init {
        mongoClient = MongoClient(MongoClientURI(uri))
        val (db, collection) = dbName.split("\\.".toRegex()).dropLastWhile(String::isEmpty)
        mongoDatabase = mongoClient.getDatabase(db)
        mongoCollection = mongoDatabase.getCollection(collection)

        log.trace("Start querying {}", dbName)
    }

    override fun run() {
        do {
            log.info("Read documents from offset {}", skipOffset)
            val documents = mongoCollection.find().skip(skipOffset).limit(bulkSize)
            try {
                for (document in documents) {
                    log.trace("Document {}", document!!.toString())
                    messages.add(getStruct(document))
                }
                skipOffset += documents.count()
            } catch (e: Exception) {
                e.printStackTrace()
                log.error("Closed connection")
            }
        } while (documents.count() > 0)
    }

    fun getStruct(document: Document): JSONObject {
        val message = JSONObject()
        val id = document["_id"] as ObjectId
        message.put("ts", id.timestamp)
        message.put("inc", 0)
        message.put("id", id.toHexString())
        message.put("database", dbName.replace("\\.".toRegex(), "_"))
        message.put("object", document.toJson())
        return message
    }
}

fun main(args: Array<String>) {
    if (args.count() < 1) throw Exception("Missing config file path!")

    val configFilePath = args[0]
    val props = Properties()
    props.load(FileInputStream(configFilePath))

    val missingKey = arrayOf(
            MongoSourceConfig.MONGO_URI_CONFIG,
            MongoSourceConfig.DATABASES_CONFIG,
            MongoSourceConfig.TOPIC_PREFIX_CONFIG).find { props[it] == null }

    if (missingKey != null) throw Exception("Missing config property: $missingKey")

    val tsLocation = props[MongoSourceConfig.TRUSTSTORE_LOCATION]
    val tsPassword = props[MongoSourceConfig.TRUSTSTORE_PASSWORD]
    if (tsLocation != null && tsPassword != null) {
        System.setProperty("javax.net.ssl.trustStore", tsLocation as String)
        System.setProperty("javax.net.ssl.trustStorePassword", tsPassword as String)
    }
    val ksLocation = props[MongoSourceConfig.KEYSTORE_LOCATION]
    val ksPassword = props[MongoSourceConfig.KEYSTORE_PASSWORD]
    if (ksLocation != null && ksPassword != null) {
        System.setProperty("javax.net.ssl.keyStore", ksLocation as String)
        System.setProperty("javax.net.ssl.keyStorePassword", ksPassword as String)
    }

    val importJob = ImportJob(
            props[MongoSourceConfig.MONGO_URI_CONFIG] as String,
            props[MongoSourceConfig.DATABASES_CONFIG] as String,
            props[MongoSourceConfig.TOPIC_PREFIX_CONFIG] as String,
            props)

    importJob.start()
}