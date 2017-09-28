package org.apache.kafka.connect.mongo

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * @author Xu Jingxin
 */
class MongoSinkConfig(props: Map<String, String>) : AbstractConfig(MongoSinkConfig.config, props) {
    companion object {
        val MONGO_URI_CONFIG = "mongo.uri"
        private val MONGO_URI_CONFIG_DOC = "Connect uri of mongodb"

        val SOURCE_TOPICS_CONFIG = "topics"
        private val SOURCE_TOPICS_CONFIG_DOC = "Topics"

        val DATABASES_CONFIG = "databases"
        private val DATABASES_CONFIG_DOC = "Databases, join database and collection with dot, split different databases with comma"

        var config = ConfigDef()
            .define(MONGO_URI_CONFIG,
                Type.STRING,
                Importance.HIGH,
                MONGO_URI_CONFIG_DOC)
            .define(DATABASES_CONFIG,
                Type.STRING,
                Importance.HIGH,
                DATABASES_CONFIG_DOC)
            .define(SOURCE_TOPICS_CONFIG,
                Type.STRING,
                Importance.HIGH,
                SOURCE_TOPICS_CONFIG_DOC)!!
    }
}
