package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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

    private MongoReader reader;
    private Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();

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
            schemas.putIfAbsent(db, SchemaBuilder
                    .struct()
                    .name(schemaName.concat("_").concat(db))
                    .field("ts", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("inc", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("object", Schema.OPTIONAL_STRING_SCHEMA)
                    .build()
            );
        }

        loadOffsets();
        reader = new MongoReader(host, port, databases, offsets);
        reader.run();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        while (!reader.messages.isEmpty() && records.size() < batchSize) {
            Document message = reader.messages.poll();
            Struct messageStruct = getStruct(message);
            records.add(new SourceRecord(
                    getPartition(getDB(message)),
                    getOffset(message),
                    getTopic(message),
                    getStruct(message).schema(),
                    messageStruct
            ));
            log.trace(message.toString());
        }
        return records;
    }

    @Override
    public void stop() {

    }

    public static Map<String, String> getPartition(String db) {
        return Collections.singletonMap("mongo", db);
    }

    private Map<String, String> getOffset(Document message) {
        BsonTimestamp timestamp = (BsonTimestamp) message.get("ts");
        String offsetVal = String.valueOf(timestamp.getTime()) + "," + timestamp.getInc();
        return Collections.singletonMap(getDB(message), offsetVal);
    }

    private String getDB(Document message) {
        return (String) message.get("ns");
    }

    private String getTopic(Document message) {
        String db = getDB(message);
        if (topicPrefix != null && !topicPrefix.isEmpty()) {
            return topicPrefix + "_" + db;
        }
        return db;
    }

    private Struct getStruct(Document message) {
        Schema schema = schemas.get(getDB(message));
        Struct messageStruct = new Struct(schema);
        BsonTimestamp bsonTimestamp = (BsonTimestamp) message.get("ts");
        messageStruct.put("ts", bsonTimestamp.getTime());
        messageStruct.put("inc", bsonTimestamp.getInc());
        messageStruct.put("id", message.get("id"));
        messageStruct.put("database", message.get("ns"));
        messageStruct.put("object", message.get("o").toString());
        return messageStruct;
    }

    private void loadOffsets() {
        List<Map<String, String>> partitions = databases.stream()
                .map(MongoSourceTask::getPartition)
                .collect(Collectors.toList());
        offsets.putAll(context.offsetStorageReader().offsets(partitions));
    }
}
