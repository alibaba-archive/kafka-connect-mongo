package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/3.
 */
public class MongoSourceTask extends SourceTask{
    private final Logger log = LoggerFactory.getLogger(MongoSourceTask.class);

    private Integer port;
    private String host;
    private String schemaName;
    private Integer batchSize;
    private String topicPrefix;
    private List<String> databases;
    private static Map<String, Schema> schemas;

    @Override
    public String version() {
        return new MongoSourceConnector().version();
    }

    /**
     * Parse the config properties into in-use type and format
     * @param props
     */
    @Override
    public void start(Map<String, String> props) {
        log.trace("Parsing configuration");

        try {
            port = Integer.parseInt(props.get(MongoSourceConnector.PORT_CONFIG));
        } catch (Exception e) {
            throw new ConnectException(MongoSourceConnector.PORT_CONFIG + " config should be an Integer");
        }

        try {
            batchSize = Integer.parseInt(props.get(MongoSourceConnector.BATCH_SIZE_CONFIG));
        } catch (Exception e) {
            throw new ConnectException(MongoSourceConnector.BATCH_SIZE_CONFIG + " config should be an Integer");
        }

        schemaName = props.get(MongoSourceConnector.SCHEMA_NAME_CONFIG);
        topicPrefix = props.get(MongoSourceConnector.SCHEMA_NAME_CONFIG);
        host = props.get(MongoSourceConnector.HOST_CONFIG);
        databases = Arrays.asList(props.get(MongoSourceConnector.DATABASES_CONFIG).split(","));

        log.trace("Creating schema");
        if (schemas == null) schemas = new HashMap<>();

        for (String db : databases) {
            db = db.replaceAll("[\\s.]", "_");
            schemas.putIfAbsent(db, SchemaBuilder
                    .struct()
                    .name(schemaName.concat("_").concat(db))
                    .field("ts", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("object", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
            );
        }

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
