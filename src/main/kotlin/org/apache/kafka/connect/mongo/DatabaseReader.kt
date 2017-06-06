package org.apache.kafka.connect.mongo

import com.mongodb.CursorType
import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import java.lang.Long.parseLong
import java.util.*

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

enum class State { READY, CLOSED }

/**
 * Connect and tail wait oplog
 * @author Xu Jingxin
 * @param uri mongodb://[user:pwd@]host:port
 * @param db mydb.test
 * @param start timestamp.inc
 * @param messages
 */
class DatabaseReader(val uri: String,
                     val db: String,
                     val start: String,
                     val messages: ConcurrentLinkedQueue<Document>) : Runnable {
    companion object {
        private val log = LoggerFactory.getLogger(DatabaseReader::class.java)
    }

    private val oplog: MongoCollection<Document>
    private val mongoClient: MongoClient
    private val mongoDatabase: MongoDatabase
    private var query: Bson? = null
    private var state = State.READY
    // Do not write documents until messages are produced into kafka
    // Reduce memory usage
    private val maxMessageSize = 2000

    init {
        val clientOptions = MongoClientOptions.builder()
                .connectTimeout(1000 * 300)
        mongoClient = MongoClient(MongoClientURI(uri, clientOptions))
        mongoDatabase = mongoClient.getDatabase("local")
        oplog = mongoDatabase.getCollection("oplog.rs")

        createQuery()

        log.trace("Start from {}", start)
    }

    override fun run() {
        log.trace("Querying oplog...")
        val documents = oplog
                .find(query)
                .sort(Document("\$natural", 1))
                .projection(Projections.include("ts", "op", "ns", "o", "o2"))
                .cursorType(CursorType.TailableAwait)
                .batchSize(100)
                .maxTime(600, TimeUnit.SECONDS)
                .maxAwaitTime(600, TimeUnit.SECONDS)
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
                    log.warn("Message overwhelm! database {}, docs {}, messages {}",
                            db,
                            count,
                            messages.size)
                    Thread.sleep(500)
                }
                if (count % 1000 == 0) {
                    log.info("Read database {}, docs {}, messages {}, memory usage {}",
                            db,
                            count,
                            messages.size,
                            Runtime.getRuntime().totalMemory())
                }
            }
        } catch (e: Exception) {
            log.error("Connection closed: {}", e.message)
            if (state != State.CLOSED) {
                throw e
            }
        }
    }

    fun stop() {
        if (state == State.CLOSED) return
        state = State.CLOSED
        try {
            mongoClient.close()
        } catch (e: Exception) {
            log.error("Close db client error: {}", e.message)
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
            val db = doc["ns"].toString().split("\\.".toRegex()).dropLastWhile(String::isEmpty)

            val nsDB = mongoClient.getDatabase(db[0])
            val nsCollection = nsDB.getCollection(db[1])
            val _id = (doc["o2"] as Document)["_id"] as ObjectId

            val docs = nsCollection.find(Filters.eq("_id", _id)).into(ArrayList<Document>())

            return if (docs.size > 0) docs[0] else null
        } catch (e: Exception) {
            e.printStackTrace()
            log.error("Can not find document: {}", doc)
        }

        return null
    }

    private fun createQuery(): Bson? {
        val timestamp = parseLong(start.split(",".toRegex())[0])
        val inc = parseLong(start.split(",".toRegex())[1])

        query = Filters.and(
                Filters.exists("fromMigrate", false),
                Filters.gt("ts", BsonTimestamp(timestamp.toInt(), inc.toInt())),
                Filters.or(
                        Filters.eq("op", "i"),
                        Filters.eq("op", "u"),
                        Filters.eq("op", "d")),
                Filters.eq("ns", db))

        return query
    }

}
