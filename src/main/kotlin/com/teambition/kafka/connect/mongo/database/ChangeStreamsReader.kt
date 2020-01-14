package com.teambition.kafka.connect.mongo.database

import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
class ChangeStreamsReader(
    val uri: String,
    val db: String,
    val start: MongoSourceOffset,
    private val messages: ConcurrentLinkedQueue<Document>
) {
    companion object {
        private val log = LoggerFactory.getLogger(ChangeStreamsReader::class.java)
    }

    fun run() {
        log.info("Start change streams reader for db: {}, start from: {}", db, start)
        val mongoDatabase = MongoClientLoader.getClient(uri).getDatabase("mydb")
        val collection = mongoDatabase.getCollection("mycoll")
        for (doc in collection.watch()) {
            println(doc)
            messages.add(doc.fullDocument)
        }
    }
}
