package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * @author Xu Jingxin
 */
class MongoSourceConfig(props: Map<String, String>) : AbstractConfig(config, props) {
    companion object {
        const val MONGO_URI_CONFIG = "mongo.uri"
        const val MONGO_URI_CONFIG_DOC = "Connect uri of mongodb"

        const val BATCH_SIZE_CONFIG = "batch.size"
        const val BATCH_SIZE_CONFIG_DOC = "Count of messages in each polling"

        const val INITIAL_IMPORT_CONFIG = "initial.import"
        private const val INITIAL_IMPORT_CONFIG_DOC = "Start import all collection before tailing"

        const val SCHEMA_NAME_CONFIG = "schema.name"
        const val SCHEMA_NAME_CONFIG_DOC = "Schema name"

        const val TOPIC_PREFIX_CONFIG = "topic.prefix"
        const val TOPIC_PREFIX_CONFIG_DOC = "Prefix of each topic, final topic will be prefix_db_collection"

        const val DATABASES_CONFIG = "databases"
        const val DATABASES_CONFIG_DOC = "Databases, join database and collection with dot, split different databases with comma"

        const val ANALYZE_SCHEMA_CONFIG = "analyze.schema"
        private const val ANALYZE_SCHEMA_DOC = "Analyze schemas of data from mongodb, save into schema registry through avro"

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
                "false",
                Importance.LOW,
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
            .define(ANALYZE_SCHEMA_CONFIG,
                Type.STRING,
                "false",
                Importance.LOW,
                ANALYZE_SCHEMA_DOC)
    }
}
