package org.apache.kafka.connect.mongo

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigDef.Importance

/**
 * @author Xu Jingxin
 */
class MongoSourceConfig(props: Map<String, String>) : AbstractConfig(MongoSourceConfig.config, props) {
    companion object {
        val MONGO_URI_CONFIG = "mongo.uri"
        private val MONGO_URI_CONFIG_DOC = "Connect uri of mongodb"

        val BATCH_SIZE_CONFIG = "batch.size"
        private val BATCH_SIZE_CONFIG_DOC = "Count of documents in each polling"

        val SCHEMA_NAME_CONFIG = "schema.name"
        private val SCHEMA_NAME_CONFIG_DOC = "Schema name"

        val TOPIC_PREFIX_CONFIG = "topic.prefix"
        private val TOPIC_PREFIX_CONFIG_DOC = "Prefix of each topic, final topic will be prefix_db_collection"

        val DATABASES_CONFIG = "databases"
        private val DATABASES_CONFIG_DOC = "Databases, join database and collection with dot, split different databases with comma"

        val TRUSTSTORE_LOCATION = "javax.net.ssl.trustStore"
        private val TRUSTSTORE_LOCATION_DOC = "Location of truststore.jks"

        val TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword"
        private val TRUSTSTORE_PASSWORD_DOC = "Password of truststore"

        val KEYSTORE_LOCATION = "javax.net.ssl.keyStore"
        private val KEYSTORE_LOCATION_DOC = "Location of keystore.jks"

        val KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword"
        private val KEYSTORE_PASSWORD_DOC = "Password of keystore"

        var config = ConfigDef()
                .define(MONGO_URI_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        MONGO_URI_CONFIG_DOC)
                .define(BATCH_SIZE_CONFIG,
                        Type.INT,
                        Importance.HIGH,
                        BATCH_SIZE_CONFIG_DOC)
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
                .define(TRUSTSTORE_LOCATION,
                        Type.STRING,
                        Importance.LOW,
                        TRUSTSTORE_LOCATION_DOC)
                .define(TRUSTSTORE_PASSWORD,
                        Type.STRING,
                        Importance.LOW,
                        TRUSTSTORE_PASSWORD_DOC)
                .define(KEYSTORE_LOCATION,
                        Type.STRING,
                        Importance.LOW,
                        KEYSTORE_LOCATION_DOC)
                .define(KEYSTORE_PASSWORD,
                        Type.STRING,
                        Importance.LOW,
                        KEYSTORE_PASSWORD_DOC)!!
    }
}
