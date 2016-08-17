package org.apache.kafka.connect.mongo;

import com.sun.deploy.util.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Connect mongodb with configs
 */
public class MongoSourceConnector extends SourceConnector{
    private final static Logger log = LoggerFactory.getLogger(MongoSourceConnector.class);

    public static final String DATABASES_CONFIG = "databases";
    public static final String HOST_CONFIG = "host";
    public static final String PORT_CONFIG = "port";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    public static final String SCHEMA_NAME_CONFIG = "schema.name";

    private String databases;
    private String host;
    private String port;
    private String batchSize;
    private String topicPrefix;
    private String schemaName;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.trace("Parsing configuration");
        port = getRequiredProp(props, PORT_CONFIG);
        databases = getRequiredProp(props, DATABASES_CONFIG);
        batchSize = getRequiredProp(props, BATCH_SIZE_CONFIG);
        host = getRequiredProp(props, HOST_CONFIG);
        topicPrefix = getRequiredProp(props, TOPIC_PREFIX_CONFIG);
        schemaName = getRequiredProp(props, SCHEMA_NAME_CONFIG);

        log.trace("Configurations {}", props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> dbs = Arrays.asList(databases.split(","));
        int numGroups = Math.min(dbs.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(dbs, numGroups);

        for (int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(PORT_CONFIG, port);
            config.put(HOST_CONFIG, host);
            config.put(DATABASES_CONFIG, StringUtils.join(dbsGrouped.get(i), ","));
            config.put(BATCH_SIZE_CONFIG, batchSize);
            config.put(TOPIC_PREFIX_CONFIG, topicPrefix);
            config.put(SCHEMA_NAME_CONFIG, schemaName);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    private String getRequiredProp(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            throw new ConnectException("Missing " + key + " config");
        }
        return value;
    }
}
