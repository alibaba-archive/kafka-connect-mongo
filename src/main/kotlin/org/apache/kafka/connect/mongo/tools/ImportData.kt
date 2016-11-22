package org.apache.kafka.connect.mongo.tools

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.apache.commons.lang.mutable.Mutable
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

/**
 * @author Xu Jingxin
 * Import all the data from one collection into kafka
 * Then save the offset
 */
class ImportJob
/**
 * @param uri mongodb://[user:pwd@]host:port
 * @param dbs Database.collection strings, combined with comma
 * @param topic Producer topic in kafka
 * @param props Kafka producer props
 */
(val uri: String,
 val dbs: String,
 val topic: String,
 val props: Properties
) {
    companion object {
        private val log = LoggerFactory.getLogger(ImportJob::class.java)
    }
    private val messages = ConcurrentLinkedQueue<Document>()
    internal val producer: KafkaProducer<String, String>

    init {
        producer = KafkaProducer(props)
    }

    /**
     * Start job
     */
    fun start() {
        log.trace("Start import data from {}", dbs)
        val threadGroup = mutableListOf<Thread>()
        dbs.split(",").dropLastWhile(String::isEmpty).forEach {
            log.trace("Import database: {}", it)
            val importDB = ImportDB(uri, it, messages)
            val t = Thread(importDB)
            threadGroup.add(t)
            t.start()
        }

        while (true) {
            val threadCount = threadGroup.filter(Thread::isAlive).count()
            if (threadCount == 0 && messages.isEmpty()) {
                break
            }
            flush()
            Thread.sleep(100)
        }

        producer.close()
    }

    /**
     * Flush messages into kafka
     */
    fun flush() {
        while (!messages.isEmpty()) {
            val message = messages.poll()
            log.trace("Poll message {}", message)
            // @todo Change structure of records
            val record = ProducerRecord<String, String>(topic, 0, message["_id"].toString(), message.toJson())
            producer.send(record)
        }
    }
}

class ImportDB
/**
 * Import data from single collection
 * @param uri mongodb://[user:pwd@]host:port
 * @param ns mydb.users
 */
(val uri: String,
 val ns: String,
 var messages: ConcurrentLinkedQueue<Document>
) : Runnable {
    private val mongoClient: MongoClient
    private val mongoDatabase: MongoDatabase
    private val mongoCollection: MongoCollection<Document>

    companion object {
        private val log = LoggerFactory.getLogger(ImportDB::class.java)
    }

    init {
        mongoClient = MongoClient(MongoClientURI(uri))
        val (db, collection) = ns.split("\\.".toRegex()).dropLastWhile(String::isEmpty)
        mongoDatabase = mongoClient.getDatabase(db)
        mongoCollection = mongoDatabase.getCollection(collection)

        log.trace("Start querying {}", ns)
    }

    override fun run() {
        val documents = mongoCollection.find()
        try {
            for (document in documents) {
                log.trace("Document {}", document!!.toString())
                messages.add(document)
            }
        } catch (e: Exception) {
            e.printStackTrace()
            log.error("Closed connection")
        }
    }
}
