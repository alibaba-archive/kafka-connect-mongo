package com.teambition.kafka.connect.mongo

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import com.teambition.kafka.connect.mongo.MongoSinkConfig.Companion.DATABASES_CONFIG
import com.teambition.kafka.connect.mongo.MongoSinkConfig.Companion.MONGO_URI_CONFIG
import com.teambition.kafka.connect.mongo.MongoSinkConfig.Companion.SOURCE_TOPICS_CONFIG
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory

/**
 * @author Xu Jingxin
 */
class MongoSinkConnector : SinkConnector() {
    companion object {
        private val log = LoggerFactory.getLogger(MongoSinkConnector::class.java)
    }

    private var uri = ""
    private var topics = ""
    private var databases = ""

    override fun version(): String = AppInfoParser.getVersion()

    override fun taskClass(): Class<out Task> = MongoSinkTask::class.java

    override fun config(): ConfigDef = MongoSinkConfig.config

    /**
     * Group tasks by number of topics
     */
    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val configs = mutableListOf<MutableMap<String, String>>()
        val topics = topics.split(",").dropLastWhile(String::isEmpty)
        val databases = databases.split(",").dropLastWhile(String::isEmpty)
        val numGroups = Math.min(topics.size, maxTasks)
        val topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups)
        val dbsGrouped = ConnectorUtils.groupPartitions(databases, numGroups)

        for (i in 0 until numGroups) {
            val config = mutableMapOf<String, String>()
            config.put(MONGO_URI_CONFIG, uri)
            config.put(SOURCE_TOPICS_CONFIG, topicsGrouped[i].joinToString(","))
            config.put(DATABASES_CONFIG, dbsGrouped[i].joinToString(","))
            configs.add(config)
        }

        return configs
    }

    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration: {}", props)
        uri = getRequiredProp(props, MONGO_URI_CONFIG)
        topics = getRequiredProp(props, SOURCE_TOPICS_CONFIG)
        databases = getRequiredProp(props, DATABASES_CONFIG)
        val topicsList = topics.split(",").dropLastWhile(String::isEmpty)
        val dbsList = databases.split(",").dropLastWhile(String::isEmpty)
        dbsList.forEach {
            val dbCollection = it.split(".")
            if (dbCollection.size != 2) {
                throw Exception("Each database's pattern should be db.collection")
            }
        }
        if (topicsList.size != dbsList.size) {
            throw Exception("Topics and databases count should be the same in sink task")
        }
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
