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
        const val BATCH_SIZE_CONFIG = "batch.size"
        const val INITIAL_IMPORT_CONFIG = "initial.import"
        const val SCHEMA_NAME_CONFIG = "schema.name"
        const val TOPIC_PREFIX_CONFIG = "topic.prefix"
        const val DATABASES_CONFIG = "databases"
        const val ANALYZE_SCHEMA_CONFIG = "analyze.schema"
        const val SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url"
        const val ADDITIONAL_FILTER = "additional.filter"

        var config: ConfigDef = ConfigDef()
            .define(
                MONGO_URI_CONFIG,
                Type.STRING,
                Importance.HIGH,
                "Connect uri of mongodb"
            )
            .define(
                BATCH_SIZE_CONFIG,
                Type.INT,
                Importance.HIGH,
                "Count of messages in each polling"
            )
            .define(
                INITIAL_IMPORT_CONFIG,
                Type.STRING,
                "false",
                Importance.LOW,
                "Start import all collection before tailing"
            )
            .define(
                SCHEMA_NAME_CONFIG,
                Type.STRING,
                Importance.HIGH,
                "Schema name"
            )
            .define(
                TOPIC_PREFIX_CONFIG,
                Type.STRING,
                Importance.HIGH,
                "Prefix of each topic, final topic will be prefix_db_collection"
            )
            .define(
                DATABASES_CONFIG,
                Type.STRING,
                Importance.HIGH,
                "Databases, join database and collection with dot, split different databases with comma"
            )
            .define(
                ANALYZE_SCHEMA_CONFIG,
                Type.STRING,
                "false",
                Importance.LOW,
                "Analyze schemas of data from mongodb, save into schema registry through avro"
            )
            .define(
                SCHEMA_REGISTRY_URL_CONFIG,
                Type.STRING,
                "false",
                Importance.LOW,
                "When analyze.schema is set to true, make sure this config is filled with corrent schema registry url"
            )
            .define(
                ADDITIONAL_FILTER,
                Type.STRING,
                "",
                Importance.LOW,
                "Add filters when exporting data from mongodb"
            )
    }
}
