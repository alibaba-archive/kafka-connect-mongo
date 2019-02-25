package com.teambition.kafka.connect.mongo.database

import com.mongodb.BasicDBObject
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
    private val messages: ConcurrentLinkedQueue<Document>,
    private val additionalFilter: BasicDBObject? = null
) {
    companion object {
        private val log = LoggerFactory.getLogger(ExportReader::class.java)
    }

    private val batchSize = 100
    private val mongoClient = MongoClientLoader.getClient(uri)
    private val maxMessageSize = 2000
    var isFinished = false

    fun run() {
        var offsetCount = 0L
        var offsetId = start.objectId
        val mongoCollection = getNSCollection(db)
        log.info("Start export reader for db {}, start from {}", db, start)
        val filter = additionalFilter
            ?.let { combineFilter(offsetId, it) }
            ?: Filters.gt("_id", offsetId)
        mongoCollection
            .find()
            .batchSize(batchSize)
            .filter(filter)
            .sort(Document("_id", 1))
            .asSequence()
            .forEach { document ->
                messages.add(formatAsOpLog(document))
                offsetId = document["_id"] as ObjectId
                offsetCount += 1
                while (messages.size > maxMessageSize) {
                    Thread.sleep(1000)
                }
                if (offsetCount % 1000 == 0L) {
                    log.info("Export processing for db {}, counts {}", db, offsetCount)
                }
            }
        log.info("Export finished for db {}, count {}", db, offsetCount)
        isFinished = true
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

    private fun combineFilter(idOffset: ObjectId, additionalFilter: BasicDBObject) =
        BasicDBObject(
            mapOf(
                "\$and" to listOf(
                    mapOf(
                        "_id" to mapOf(
                            "\$gt" to idOffset
                        )
                    ),
                    additionalFilter
                )
            )
        )
}
