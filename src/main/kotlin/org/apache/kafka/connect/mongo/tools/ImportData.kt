package org.apache.kafka.connect.mongo.tools

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 * Import all the data from one collection into kafka
 * Then save the offset
 */
class ImportJob
(val dbs: String
) {
    fun run() {
        print("Start import data")
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
