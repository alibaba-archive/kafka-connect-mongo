package org.apache.kafka.connect.mongo

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.apache.kafka.connect.mongo.interfaces.DatabaseRunner
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Export the whole collection into message queue
 * @author Xu Jingxin
 */
class CollectionExporter(val uri: String,
                         val db: String,
                         val messages: ConcurrentLinkedQueue<Document>): DatabaseRunner(uri, db, messages) {
    companion object {
        private val log = LoggerFactory.getLogger(CollectionExporter::class.java)
    }

    private val mongoClient = MongoClient(MongoClientURI(uri))
    private val mongoDatabase: MongoDatabase
    private val mongoCollection: MongoCollection<Document>
    private val bulkSize = 1000
    private val maxSize = 3000
    private var count = 0

    init {
        val (db, collection) = db.trim().split(".")
        mongoDatabase = mongoClient.getDatabase(db)
        mongoCollection = mongoDatabase.getCollection(collection)
    }

    override fun run() {
        var offsetId: ObjectId? = null
        do {
            log.info("Read messages at $db from offset {}, count {}", offsetId, count)
            val iterator = mongoCollection.find()
            if (offsetId != null) {
                iterator.filter(Filters.gt("_id", offsetId))
            }
            iterator.sort(Document("_id", 1))
                    .limit(bulkSize)

            try {
                for (document in iterator) {
                    messages.add(document)
                    offsetId = document["_id"] as ObjectId
                    count += 1
                }
                // Throttling
                while (messages.size > maxSize) {
                    log.warn("Message overwhelm! database {}, docs {}, messages {}",
                            db,
                            count,
                            messages.size)
                    Thread.sleep(500)
                }
            } catch (e: Exception) {
                log.error("Querying error: {}", e.message)
            }
        } while (iterator.count() > 0)
        stop()
    }

    override fun stop() {
        try {
            mongoClient.close()
        } catch (e: Exception) {
            log.error("Close db client error: {}", e.message)
        }
        log.info("Export database {} finish, documents count is {}",
                db,
                count)
    }
}