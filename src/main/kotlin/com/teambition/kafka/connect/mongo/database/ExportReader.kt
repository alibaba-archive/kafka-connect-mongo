package com.teambition.kafka.connect.mongo.database

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
class ExportReader(
    val uri: String,
    val db: String,
    val start: MongoSourceOffset,
    private val messages: ConcurrentLinkedQueue<Document>
) {
    companion object {
        private val log = LoggerFactory.getLogger(ExportReader::class.java)
    }

    private val batchSize = 100
    private val mongoClient = MongoClientLoader.getClient(uri)
    private val maxMessageSize = 2000

    fun run() {
        var offsetCount = 0L
        var offsetId = start.objectId
        val mongoCollection = getNSCollection(db)
        log.info("Start export reader for db {}, start from {}", db, offsetId)
        mongoCollection
            .find()
            .batchSize(batchSize)
            .filter(Filters.gt("_id", offsetId))
            .sort(Document("_id", 1))
            .asSequence()
            .forEach { document ->
                messages.add(formatAsOpLog(document))
                offsetId = document["_id"] as ObjectId
                offsetCount += 1
                while (messages.size > maxMessageSize) {
                    log.warn(
                        "Message overwhelm! database {}, docs {}, messages {}",
                        db,
                        offsetCount,
                        messages.size
                    )
                    Thread.sleep(500)
                }
            }
        log.info("Export finished for db {}, count {}", db, offsetCount)
    }

    private fun getNSCollection(ns: String): MongoCollection<Document> {
        val dbAndCollection = ns.split("\\.".toRegex()).dropLastWhile(String::isEmpty)
        val nsDB = mongoClient.getDatabase(dbAndCollection[0])
        return nsDB.getCollection(dbAndCollection[1])
    }

    private fun formatAsOpLog(doc: Document): Document {
        val opDoc = Document()
        opDoc["ts"] = BsonTimestamp((Date().time / 1000).toInt(), 0)
        opDoc["ns"] = db
        opDoc["initialImport"] = true
        opDoc["op"] = "i"  // Treat as insertion
        opDoc["o"] = doc
        return opDoc
    }
}
