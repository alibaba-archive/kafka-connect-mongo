package com.teambition.kafka.connect.mongo.source

import org.slf4j.LoggerFactory

/**
 * @author Xu Jingxin
 */
class MongoExportSourceTask : AbstractMongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoExportSourceTask::class.java)
}
