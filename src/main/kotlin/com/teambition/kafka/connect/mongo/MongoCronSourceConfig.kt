package com.teambition.kafka.connect.mongo

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Type.INT
import org.apache.kafka.common.config.ConfigDef.Type.STRING
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.BATCH_SIZE_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.BATCH_SIZE_CONFIG_DOC
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.DATABASES_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.DATABASES_CONFIG_DOC
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.MONGO_URI_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.MONGO_URI_CONFIG_DOC
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.SCHEMA_NAME_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.SCHEMA_NAME_CONFIG_DOC
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.TOPIC_PREFIX_CONFIG
import com.teambition.kafka.connect.mongo.MongoSourceConfig.Companion.TOPIC_PREFIX_CONFIG_DOC

/**
 * @author Xu Jingxin
 */
class MongoCronSourceConfig(props: Map<String, String>) : AbstractConfig(MongoCronSourceConfig.config, props) {
    companion object {
        const val SCHEDULE_CONFIG = "schedule"
        private const val SCHEDULE_CONFIG_DOC = "Schedule job in quartz cron pattern"

        val config: ConfigDef = ConfigDef()
            .define(MONGO_URI_CONFIG,
                STRING,
                HIGH,
                MONGO_URI_CONFIG_DOC)
            .define(BATCH_SIZE_CONFIG,
                INT,
                HIGH,
                BATCH_SIZE_CONFIG_DOC)
            .define(SCHEMA_NAME_CONFIG,
                STRING,
                HIGH,
                SCHEMA_NAME_CONFIG_DOC)
            .define(TOPIC_PREFIX_CONFIG,
                STRING,
                HIGH,
                TOPIC_PREFIX_CONFIG_DOC)
            .define(DATABASES_CONFIG,
                STRING,
                HIGH,
                DATABASES_CONFIG_DOC)
            .define(SCHEDULE_CONFIG,
                STRING,
                HIGH,
                SCHEDULE_CONFIG_DOC)
    }
}
