package com.teambition.kafka.connect.mongo.database

import com.mongodb.MongoCommandException
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.OperationType
import com.teambition.kafka.connect.mongo.source.Message
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
class ChangeStreamsReader(
    val uri: String,
    val db: String,
    val start: MongoSourceOffset,
    private val messages: ConcurrentLinkedQueue<Message>
) {
    companion object {
        private val log = LoggerFactory.getLogger(ChangeStreamsReader::class.java)
    }

    fun run() {
        log.info("Start change streams reader for db: {}, start from: {}", db, start)
        val (db, coll) = db.split(".")
        val mongoDatabase = MongoClientLoader.getClient(uri).getDatabase(db)
        val collection = mongoDatabase.getCollection(coll)

        try {
            val watch = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP)
            start.resumeToken?.let { watch.resumeAfter(it) }
            watch.forEach(this::handleOps)
        } catch (e: MongoCommandException) {
            log.error(e.message)
            log.warn("Resume from timestamp {}", start.ts)
            val watch = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP)
                .startAtOperationTime(start.ts)
            watch.forEach(this::handleOps)
        }
    }

    private fun handleOps(doc: ChangeStreamDocument<Document>) {
        val oplog = formatAsOplog(doc)
        if (oplog != null) {
            messages.add(
                Message(
                    getOffset(oplog, doc.resumeToken),
                    oplog
                )
            )
            while (messages.size > 2000) {
                log.info(
                    "Read database {}, messages queue size {}, memory usage {}",
                    db,
                    messages.size,
                    Runtime.getRuntime().totalMemory()
                )
                Thread.sleep(1000)
            }
        }
    }

    private fun formatAsOplog(doc: ChangeStreamDocument<Document>): Document? {
        val oplog = Document()
        // clusterTime only provided after mongodb 3.8
        oplog["ts"] = doc.clusterTime ?: BsonTimestamp((Date().time / 1000).toInt(), 0)
        oplog["ns"] = doc.namespace!!.toString()
        oplog["op"] = when (doc.operationType) {
            OperationType.INSERT -> "i"
            OperationType.UPDATE -> "u"
            OperationType.DELETE -> "d"
            else -> return null  // Drop documents in other operations
        }
        oplog["o"] = when (doc.operationType) {
            OperationType.INSERT, OperationType.UPDATE -> doc.fullDocument
            OperationType.DELETE -> Document("_id", doc.documentKey?.getObjectId("_id")?.value)
            else -> return null
        }
        return oplog
    }

    private fun getOffset(oplog: Document, resumeToken: BsonDocument) =
        MongoSourceOffset(oplog, true).also {
            it.resumeToken = resumeToken
        }
}
