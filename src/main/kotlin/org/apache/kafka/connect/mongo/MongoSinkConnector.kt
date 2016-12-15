package org.apache.kafka.connect.mongo

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory
import org.apache.kafka.connect.mongo.MongoSinkConfig.Companion.MONGO_URI_CONFIG
import org.apache.kafka.connect.mongo.MongoSinkConfig.Companion.SOURCE_TOPICS_CONFIG

/**
 * @author Xu Jingxin
 */
class MongoSinkConnector: SinkConnector() {
    companion object {
        private val log = LoggerFactory.getLogger(MongoSinkConnector::class.java)
    }
    private var uri = ""
    private var topics = ""

    override fun version(): String = AppInfoParser.getVersion()

    override fun taskClass(): Class<out Task> = MongoSinkTask::class.java

    override fun config(): ConfigDef = MongoSinkConfig.config

    /**
     * Group tasks by number of topics
     */
    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val configs = mutableListOf<MutableMap<String, String>>()
        val topics = topics.split(",").dropLastWhile(String::isEmpty)
        val numGroups = Math.min(topics.size, maxTasks)
        val topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups)

        for (i in 0..numGroups - 1) {
            val config = mutableMapOf<String, String>()
            config.put(MONGO_URI_CONFIG, uri)
            config.put(SOURCE_TOPICS_CONFIG, topicsGrouped[i].joinToString(","))
            configs.add(config)
        }

        return configs
    }

    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration")
        uri = getRequiredProp(props, MONGO_URI_CONFIG)
        topics = getRequiredProp(props, SOURCE_TOPICS_CONFIG)
        log.trace("Configurations {}", props)
    }

    override fun stop() {
    }

    private fun getRequiredProp(props: Map<String, String>, key: String): String {
        val value = props[key]
        if (value == null || value.isEmpty()) {
            throw ConnectException("Missing $key config")
        }
        return value
    }
}