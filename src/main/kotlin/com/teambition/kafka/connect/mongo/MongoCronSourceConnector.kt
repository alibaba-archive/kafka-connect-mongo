package com.teambition.kafka.connect.mongo

import org.apache.kafka.common.config.ConfigDef
import com.teambition.kafka.connect.mongo.MongoCronSourceConfig.Companion.SCHEDULE_CONFIG
import org.slf4j.LoggerFactory

/**
 * @author Xu Jingxin
 */
class MongoCronSourceConnector : MongoSourceConnector() {
    override val log = LoggerFactory.getLogger(MongoCronSourceConnector::class.java)!!
    private var schedule: String = ""

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val configs = super.taskConfigs(maxTasks)
        configs.forEach {
            it.put(SCHEDULE_CONFIG, schedule)
        }
        return configs
    }

    override fun start(props: Map<String, String>) {
        super.start(props)
        schedule = getRequiredProp(props, SCHEDULE_CONFIG)
    }

    override fun taskClass() = MongoCronSourceTask::class.java

    override fun config(): ConfigDef = MongoCronSourceConfig.config
}
