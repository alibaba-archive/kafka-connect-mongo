package com.teambition.kafka.connect.mongo.source

import com.mongodb.BasicDBObject
import com.teambition.kafka.connect.mongo.database.ExportReader
import com.teambition.kafka.connect.mongo.source.MongoSourceConfig.Companion.ADDITIONAL_FILTER
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.LoggerFactory
import kotlin.concurrent.thread
import kotlin.system.exitProcess

/**
 * @author Xu Jingxin
 */
class MongoExportSourceTask : AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoExportSourceTask::class.java)!!
    private var additionalFilter: BasicDBObject? = null
    private val exportReaders = mutableMapOf<String, ExportReader>()

    override fun start(props: Map<String, String>) {
        super.start(props)
        additionalFilter = props[ADDITIONAL_FILTER]
            ?.takeIf { it.isNotEmpty() }
            ?.let { BasicDBObject.parse(it) }

        databases.forEach { db ->
            val partition = getPartition(db)
            val recordedOffset = context.offsetStorageReader().offset(partition)
            val startOffset =
                if (!(recordedOffset == null || recordedOffset.isEmpty())) recordedOffset[db] as String else null

            val start = if (startOffset != null) MongoSourceOffset(startOffset, db) else MongoSourceOffset()
            val reader = ExportReader(uri, db, start, messages, additionalFilter)
            exportReaders[db] = reader
            thread {
                reader.run()
            }.also {
                it.uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, e -> this.unrecoverable = e }
            }
        }
    }

    override fun poll(): List<SourceRecord> {
        if (messages.isEmpty() && allFinished()) {
            log.info("Export finished, exit in 60 seconds.")
            Thread.sleep(60000)
            exitProcess(0)
        }
        return super.poll()
    }

    /**
     * Check if all reader have finished their jobs
     */
    private fun allFinished() =
        exportReaders.values.firstOrNull { !it.isFinished } == null
}
