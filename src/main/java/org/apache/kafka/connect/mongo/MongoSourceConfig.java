package org.apache.kafka.connect.mongo;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * @author Xu Jingxin
 */
public class MongoSourceConfig extends AbstractConfig {
    private static final String HOST_CONFIG = "host";
    private static final String HOST_CONFIG_DOC = "Host url of mongodb";
    private static final String PORT_CONFIG = "port";
    private static final String PORT_CONFIG_DOC = "Port of mongodb";
    private static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_CONFIG_DOC = "Count of documents in each polling";
    private static final String SCHEMA_NAME_CONFIG = "schema.name";
    private static final String SCHEMA_NAME_CONFIG_DOC = "Schema name";
    private static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_CONFIG_DOC = "Prefix of each topic, final topic will be prefix_db_collection";
    private static final String DATABASES_CONFIG = "databases";
    private static final String DATABASES_CONFIG_DOC = "Databases, join database and collection with dot, split different databases with comma";

    public static ConfigDef config = new ConfigDef()
            .define(HOST_CONFIG, Type.STRING, Importance.HIGH, HOST_CONFIG_DOC)
            .define(PORT_CONFIG, Type.INT, Importance.HIGH, PORT_CONFIG_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, Importance.HIGH, BATCH_SIZE_CONFIG_DOC)
            .define(SCHEMA_NAME_CONFIG, Type.STRING, Importance.HIGH, SCHEMA_NAME_CONFIG_DOC)
            .define(TOPIC_PREFIX_CONFIG, Type.STRING, Importance.HIGH, TOPIC_PREFIX_CONFIG_DOC)
            .define(DATABASES_CONFIG, Type.STRING, Importance.HIGH, DATABASES_CONFIG_DOC);

    public MongoSourceConfig(Map<String, String> props) {
        super(config, props);
    }
}
