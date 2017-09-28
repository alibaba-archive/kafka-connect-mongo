package org.apache.kafka.connect.mongo

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * @author Xu Jingxin
 */
class MongoSourceConfig(props: Map<String, String>) : AbstractConfig(MongoSourceConfig.config, props) {
    companion object {
        val MONGO_URI_CONFIG = "mongo.uri"
        val MONGO_URI_CONFIG_DOC = "Connect uri of mongodb"

        val BATCH_SIZE_CONFIG = "batch.size"
        val BATCH_SIZE_CONFIG_DOC = "Count of messages in each polling"

        val INITIAL_IMPORT_CONFIG = "initial.import"
        val INITIAL_IMPORT_CONFIG_DOC = "Start import all collection before tailing"

        val SCHEMA_NAME_CONFIG = "schema.name"
        val SCHEMA_NAME_CONFIG_DOC = "Schema name"

        val TOPIC_PREFIX_CONFIG = "topic.prefix"
        val TOPIC_PREFIX_CONFIG_DOC = "Prefix of each topic, final topic will be prefix_db_collection"

        val DATABASES_CONFIG = "databases"
        val DATABASES_CONFIG_DOC = "Databases, join database and collection with dot, split different databases with comma"

        var config: ConfigDef = ConfigDef()
                .define(MONGO_URI_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        MONGO_URI_CONFIG_DOC)
                .define(BATCH_SIZE_CONFIG,
                        Type.INT,
                        Importance.HIGH,
                        BATCH_SIZE_CONFIG_DOC)
                .define(INITIAL_IMPORT_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        INITIAL_IMPORT_CONFIG_DOC)
                .define(SCHEMA_NAME_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        SCHEMA_NAME_CONFIG_DOC)
                .define(TOPIC_PREFIX_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        TOPIC_PREFIX_CONFIG_DOC)
                .define(DATABASES_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        DATABASES_CONFIG_DOC)
    }
}
