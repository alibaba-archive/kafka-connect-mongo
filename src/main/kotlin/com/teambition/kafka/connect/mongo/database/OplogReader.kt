package com.teambition.kafka.connect.mongo.database

import com.mongodb.CursorType
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

/**
 * Connect and tail wait oplog
 * @author Xu Jingxin
 * @param uri mongodb://[user:pwd@]host:port
 * @param db mydb.test
 * @param start
 * @param messages
 */
class OplogReader(
    val uri: String,
    val db: String,
    val start: MongoSourceOffset,
    private val messages: ConcurrentLinkedQueue<Document>
) {
    companion object {
        private val log = LoggerFactory.getLogger(OplogReader::class.java)
    }

    private val batchSize = 100
    private val oplog: MongoCollection<Document>
    private val mongoClient: MongoClient = MongoClientLoader.getClient(uri)
    private val mongoDatabase: MongoDatabase
    private var query: Bson
    // Do not write documents until messages are produced into kafka
    // Reduce memory usage
    private val maxMessageSize = 2000

    init {
        mongoDatabase = mongoClient.getDatabase("local")
        oplog = mongoDatabase.getCollection("oplog.rs")
        query = buildQuery()
    }

    fun run() {
        log.info("Start oplog reader for db: {}, start from: {}", db, start)
        val documents = oplog
            .find(query)
            .sort(Document("\$natural", 1))
            .projection(Projections.include("ts", "op", "ns", "o", "o2"))
            .cursorType(CursorType.TailableAwait)
            .batchSize(batchSize)
            .maxTime(3600, TimeUnit.SECONDS)
            .maxAwaitTime(3600, TimeUnit.SECONDS)
            .oplogReplay(true)

        var count = 0
        try {
            for (document in documents) {
                log.trace("Document {}", document!!.toString())
                val doc = handleOp(document)
                count += 1
                if (doc != null) messages.add(doc)
                // Stop pulling data when length of message is too large!
                while (messages.size > maxMessageSize) {
                    Thread.sleep(1000)
                }
                if (count % 1000 == 0) {
                    log.info(
                        "Read database {}, docs {}, messages {}, memory usage {}",
                        db,
                        count,
                        messages.size,
                        Runtime.getRuntime().totalMemory()
                    )
                }
            }
        } catch (e: Exception) {
            log.error("Connection closed: {}", e.message)
            throw e
        }
    }

    /**
     * Handle operations
     * i: keep oplog
     * u: find origin document
     * d: keep oplog
     * @param doc oplog
     * *
     * @return Document
     */
    private fun handleOp(doc: Document): Document? {
        when (doc["op"] as String) {
            "u" -> {
                val updated = findOneById(doc) ?: return null
                doc.append("o", updated)
            }
            else -> {
            }
        }
        return doc
    }

    private fun findOneById(doc: Document): Document? {
        try {
            val nsCollection = getNSCollection(doc["ns"].toString())
            val id = (doc["o2"] as Document)["_id"] as ObjectId

            val docs = nsCollection.find(Filters.eq("_id", id)).into(ArrayList<Document>())

            return if (docs.size > 0) docs[0] else null
        } catch (e: Exception) {
            e.printStackTrace()
            log.error("Can not find document: {}", doc)
        }

        return null
    }

    private fun getNSCollection(ns: String): MongoCollection<Document> {
        val dbAndCollection = ns.split("\\.".toRegex()).dropLastWhile(String::isEmpty)
        val nsDB = mongoClient.getDatabase(dbAndCollection[0])
        return nsDB.getCollection(dbAndCollection[1])
    }

    private fun buildQuery() =
        Filters.and(
            Filters.exists("fromMigrate", false),
            Filters.gt("ts", start.ts),
            Filters.or(
                Filters.eq("op", "i"),
                Filters.eq("op", "u"),
                Filters.eq("op", "d")
            ),
            Filters.eq("ns", db)
        )

}
