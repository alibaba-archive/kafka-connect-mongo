package org.apache.kafka.connect.mongo

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Export the whole collection into message queue
 * @author Xu Jingxin
 */
class CollectionExporter : Job {
    companion object {
        private val log = LoggerFactory.getLogger(CollectionExporter::class.java)
    }

    private var uri = ""
    private var db = ""
    private var messages = ConcurrentLinkedQueue<Document>()
    private var mongoClient: MongoClient? = null
    private var mongoDatabase: MongoDatabase? = null
    private var mongoCollection: MongoCollection<Document>? = null
    private val bulkSize = 1000
    private val maxSize = 3000
    private var count = 0

    override fun execute(context: JobExecutionContext) {
        init(context)
        run()
    }

    fun init(context: JobExecutionContext) {
        uri = context.mergedJobDataMap["uri"] as String
        db = context.mergedJobDataMap["db"] as String
        log.info("Init collection exporter, uri {}, db {}", uri, db)
        messages = context.mergedJobDataMap["messages"] as ConcurrentLinkedQueue<Document>
        val clientOptions = MongoClientOptions.builder()
                .connectTimeout(1000 * 300)
        mongoClient = MongoClient(MongoClientURI(uri, clientOptions))
        val (db, collection) = db.trim().split(".")
        mongoDatabase = mongoClient!!.getDatabase(db)
        mongoCollection = mongoDatabase!!.getCollection(collection)
    }

    fun run() {
        var offsetId: ObjectId? = null
        do {
            log.info("Read messages at $db from offset {}, count {}", offsetId, count)
            val iterator = mongoCollection!!.find()
            if (offsetId != null) {
                iterator.filter(Filters.gt("_id", offsetId))
            }
            iterator.sort(Document("_id", 1))
                    .limit(bulkSize)

            try {
                for (document in iterator) {
                    messages.add(getPayload(document))
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

    fun stop() {
        try {
            mongoClient?.close()
        } catch (e: Exception) {
            log.error("Close db client error: {}", e.message)
        }
        log.info("Export database {} finish, documents count is {}",
                db,
                count)
    }

    private fun getPayload(doc: Document): Document {
        return Document(mapOf(
                "ts" to BsonTimestamp(Math.floor((Date().time / 1000).toDouble()).toInt(), 0),
                "op" to "i",
                "ns" to db,
                "o" to doc
        ))
    }
}
