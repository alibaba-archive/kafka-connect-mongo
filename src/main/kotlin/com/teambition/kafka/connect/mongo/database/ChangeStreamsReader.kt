package com.teambition.kafka.connect.mongo.database

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
        val resumeToken = start.resumeToken
        log.info("Start change streams reader for db: {}, start from: {}", db, resumeToken)
        val (db, coll) = db.split(".")
        val mongoDatabase = MongoClientLoader.getClient(uri).getDatabase(db)
        val collection = mongoDatabase.getCollection(coll)
        val watch = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP)
        if (resumeToken != null) {
            watch.resumeAfter(resumeToken)
        }
        var count = 0
        for (doc in watch) {
            val oplog = formatAsOplog(doc)
            if (oplog != null) {
                count += 1
                messages.add(
                    Message(
                        getOffset(oplog, doc.resumeToken),
                        oplog
                    )
                )
                while (messages.size > 2000) {
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
