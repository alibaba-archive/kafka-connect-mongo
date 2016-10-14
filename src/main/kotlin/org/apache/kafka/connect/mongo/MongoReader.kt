package org.apache.kafka.connect.mongo

import org.bson.Document
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Create a new DatabaseReader thread for each db

 * @author Xu Jingxin
 */
class MongoReader(private val host: String,
                  private val port: Int,
                  private val dbs: List<String>,
                  private val offsets: Map<Map<String, String>, Map<String, Any>>) {
    companion object {
        private val log = LoggerFactory.getLogger(MongoReader::class.java)
    }

    internal var messages: ConcurrentLinkedQueue<Document>

    init {
        this.messages = ConcurrentLinkedQueue<Document>()
    }

    fun run() {
        for (db in dbs) {
            var start = "0,0"
            val timeOffset = this.offsets[MongoSourceTask.getPartition(db)]
            if (!(timeOffset == null || timeOffset.isEmpty())) start = timeOffset[db] as String
            log.trace("Starting database reader with configuration: ")
            log.trace("host: {}", host)
            log.trace("port: {}", port)
            log.trace("db: {}", db)
            log.trace("start: {}", timeOffset)
            val reader = DatabaseReader(host, port, db, start, messages)
            Thread(reader).start()
        }
    }
}
