package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.mongo.interfaces.AbstractMongoSourceTask
import org.slf4j.LoggerFactory

/**
* @author Xu Jingxin
*/
class MongoSourceTask: AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoSourceTask::class.java)!!
    // How many times will a process retries before quit
    private val maxErrCount = 5
    private val databaseReaders = mutableMapOf<String, DatabaseReader>()

    override fun start(props: Map<String, String>) {
        super.start(props)
        databases.forEach {startReader(it, 0)}
    }

    /**
     * Disconnect mongo client and cleanup messages
     */
    override fun stop() {
        log.info("Graceful stop mongo source task")
        databaseReaders.forEach { _, reader ->
            reader.stop()
        }
        messages.clear()
    }

    private fun loadReader(db: String): DatabaseReader {
        var start = "0,0"
        val partition = getPartition(db)
        val timeOffset = context.offsetStorageReader().offset(partition)
        if (!(timeOffset == null || timeOffset.isEmpty())) start = timeOffset[db] as String
        log.info("Start database reader for db: {}, start from: {}",
                db,
                start)
        return DatabaseReader(uri, db, start, messages)
    }

    // Create a new DatabaseReader thread for
    private fun startReader(db: String, errCount: Int = 0) {
        if (errCount > maxErrCount) throw Exception("Can not execute database reader!")
        val startTime = System.currentTimeMillis()
        val reader = loadReader(db)
        databaseReaders[db] = reader
        val t = Thread(reader)
        val uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { thread, throwable ->
            throwable.printStackTrace()
            reader.stop()
            log.error("Error when read data from db: {}", db)
            var _errCount = errCount + 1
            // Reset error count if the task executed more than 5 minutes
            if ((System.currentTimeMillis() - startTime) > 600000) {
               _errCount = 0
            }
            startReader(db, _errCount)
        }
        t.uncaughtExceptionHandler = uncaughtExceptionHandler
        t.start()
    }
}
