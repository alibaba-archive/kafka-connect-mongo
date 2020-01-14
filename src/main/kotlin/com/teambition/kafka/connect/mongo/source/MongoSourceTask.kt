package com.teambition.kafka.connect.mongo.source

import com.teambition.kafka.connect.mongo.database.ExportReader
import com.teambition.kafka.connect.mongo.database.OplogReader
import com.teambition.kafka.connect.mongo.utils.TaskUtil
import org.slf4j.LoggerFactory
import kotlin.concurrent.thread

/**
 * @author Xu Jingxin
 */
class MongoSourceTask : AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoSourceTask::class.java)!!

    override fun start(props: Map<String, String>) {
        super.start(props)
        databases.forEach { db ->
            TaskUtil.runTry(db, 3000) {
                val partition = getPartition(db)
                val recordedOffset = context.offsetStorageReader().offset(partition)
                val startOffset =
                    if (!(recordedOffset == null || recordedOffset.isEmpty())) recordedOffset[db] as String else null
                val start = if (startOffset != null) MongoSourceOffset(startOffset) else MongoSourceOffset()
                thread {
                    if (!start.finishedImport && initialImport) {
                        ExportReader(uri, db, start, messages).run()
                    }
                    OplogReader(uri, db, start, messages).run()
                }.also {
                    it.uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, e -> this.unrecoverable = e }
                }
            }
        }
    }
}
