package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.connect.connector.Task

/**
 * @author Xu Jingxin
 * Don't remove this class
 */
class MongoExportSourceConnector : MongoSourceConnector() {
    override fun taskClass(): Class<out Task> = MongoExportSourceTask::class.java
}
