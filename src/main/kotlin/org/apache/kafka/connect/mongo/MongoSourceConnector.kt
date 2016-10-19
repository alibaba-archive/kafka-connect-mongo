package org.apache.kafka.connect.mongo

import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory

import java.util.*
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.DATABASES_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.BATCH_SIZE_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.MONGO_URI_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.TOPIC_PREFIX_CONFIG
import org.apache.kafka.connect.mongo.MongoSourceConfig.Companion.SCHEMA_NAME_CONFIG

/**
 * Connect mongodb with configs
 */
class MongoSourceConnector : SourceConnector() {
    companion object {
        private val log = LoggerFactory.getLogger(MongoSourceConnector::class.java)
    }
    private var databases: String = ""
    private var uri: String = ""
    private var batchSize: String = ""
    private var topicPrefix: String = ""
    private var schemaName: String = ""

    override fun version(): String {
        return AppInfoParser.getVersion()
    }

    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration")
        databases = getRequiredProp(props, DATABASES_CONFIG)
        batchSize = getRequiredProp(props, BATCH_SIZE_CONFIG)
        uri = getRequiredProp(props, MONGO_URI_CONFIG)
        topicPrefix = getRequiredProp(props, TOPIC_PREFIX_CONFIG)
        schemaName = getRequiredProp(props, SCHEMA_NAME_CONFIG)

        log.trace("Configurations {}", props)
    }

    override fun taskClass(): Class<out Task> {
        return MongoSourceTask::class.java
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        val configs = ArrayList<Map<String, String>>()
        val dbs = Arrays.asList<String>(*databases.split(",".toRegex()).dropLastWhile(String::isEmpty).toTypedArray())
        val numGroups = Math.min(dbs.size, maxTasks)
        val dbsGrouped = ConnectorUtils.groupPartitions(dbs, numGroups)

        for (i in 0..numGroups - 1) {
            val config = HashMap<String, String>()
            config.put(MONGO_URI_CONFIG, uri)
            config.put(DATABASES_CONFIG, StringUtils.join(dbsGrouped[i], ","))
            config.put(BATCH_SIZE_CONFIG, batchSize)
            config.put(TOPIC_PREFIX_CONFIG, topicPrefix)
            config.put(SCHEMA_NAME_CONFIG, schemaName)
            configs.add(config)
        }
        return configs
    }

    override fun stop() {

    }

    override fun config(): ConfigDef {
        return MongoSourceConfig.config
    }

    private fun getRequiredProp(props: Map<String, String>, key: String): String {
        val value = props[key]
        if (value == null || value.isEmpty()) {
            throw ConnectException("Missing $key config")
        }
        return value
    }
}
