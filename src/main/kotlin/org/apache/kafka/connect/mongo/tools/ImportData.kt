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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.bson.types.ObjectId
import java.io.FileInputStream
import java.util.*

/**
 * @author Xu Jingxin
 * Import all the data from one collection into kafka
 * Then save the offset *
 * @param uri mongodb://[user:pwd@]host:port
 * @param dbs Database.collection strings, combined with comma
 * @param topic Producer topic in kafka
 * @param props Kafka producer props
 */
class ImportJob(val uri: String,
                val dbs: String,
                val topic: String,
                val props: Properties) {

    companion object {
        private val log = LoggerFactory.getLogger(ImportJob::class.java)
    }
    private val messages = ConcurrentLinkedQueue<Struct>()
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
            val importDB = ImportDB(uri, it, messages)
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
               var messages: ConcurrentLinkedQueue<Struct>) : Runnable {

    private val mongoClient: MongoClient
    private val mongoDatabase: MongoDatabase
    private val mongoCollection: MongoCollection<Document>

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
        val documents = mongoCollection.find()
        try {
            for (document in documents) {
                log.trace("Document {}", document!!.toString())
                messages.add(getStruct(document))
            }
        } catch (e: Exception) {
            e.printStackTrace()
            log.error("Closed connection")
        }
    }

    fun getStruct(document: Document): Struct {
        val schema = SchemaBuilder.struct()
                .field("ts", Schema.OPTIONAL_INT32_SCHEMA)
                .field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                .field("object", Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        val struct = Struct(schema)
        val id = document["_id"] as ObjectId
        struct.put("ts", id.timestamp)
        struct.put("inc", 0)
        struct.put("id", id.toHexString())
        struct.put("database", dbName.replace("\\.".toRegex(), "_"))
        struct.put("object", document.toJson())
        return struct
    }
}

fun main(args: Array<String>) {
    if (args.count() < 1) throw Exception("Missing config file path!")

    val configFilePath = args[0]
    val props = Properties()
    props.load(FileInputStream(configFilePath))


    println(props)

}