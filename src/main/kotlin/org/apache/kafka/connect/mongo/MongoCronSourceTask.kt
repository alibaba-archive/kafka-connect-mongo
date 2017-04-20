package org.apache.kafka.connect.mongo

import org.slf4j.LoggerFactory
import org.apache.kafka.connect.mongo.MongoCronSourceConfig.Companion.SCHEDULE_CONFIG

/**
 * @author Xu Jingxin
 */
class MongoCronSourceTask: MongoSourceTask() {
    override val log = LoggerFactory.getLogger(MongoCronSourceTask::class.java)!!
    var schedule = ""

    override fun start(props: Map<String, String>) {
        super.start(props)
        schedule = props[SCHEDULE_CONFIG] ?: throw Exception("Invalid schedule!")
    }
}