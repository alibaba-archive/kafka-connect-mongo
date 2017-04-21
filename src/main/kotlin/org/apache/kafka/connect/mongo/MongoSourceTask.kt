package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.mongo.interfaces.AbstractMongoSourceTask
import org.apache.kafka.connect.mongo.interfaces.DatabaseRunner
import org.slf4j.LoggerFactory

/**
* @author Xu Jingxin
*/
open class MongoSourceTask: AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoSourceTask::class.java)!!

    override fun version(): String = MongoSourceConnector().version()

    override fun loadDatabaseRunner(db: String): DatabaseRunner {
        var start = "0,0"
        val partition = getPartition(db)
        val timeOffset = context.offsetStorageReader().offset(partition)
        if (!(timeOffset == null || timeOffset.isEmpty())) start = timeOffset[db] as String
        log.info("Start database reader for db: {}, start from: {}",
                db,
                start)
        return DatabaseReader(uri, db, start, messages)
    }
}
