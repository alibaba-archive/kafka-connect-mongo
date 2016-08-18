package org.apache.kafka.connect.mongo;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Create a new DatabaseReader thread for each db
 *
 * @author Xu Jingxin
 */
public class MongoReader {

    private static final Logger log = LoggerFactory.getLogger(MongoReader.class);

    private String host;
    private Integer port;
    private List<String> dbs;
    private Map<Map<String, String>, Map<String, Object>> offsets;

    ConcurrentLinkedQueue<Document> messages;

    public MongoReader(String host,
                       Integer port,
                       List<String> dbs,
                       Map<Map<String, String>, Map<String, Object>> offsets) {
        this.host = host;
        this.port = port;
        this.dbs = dbs;
        this.offsets = offsets;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        for (String db: dbs) {
            String start = "0.0";
            Map<String, Object> timeOffset = this.offsets.get(
                    MongoSourceTask.getPartition(db)
            );
            if (!(timeOffset == null || timeOffset.isEmpty())) start = (String) timeOffset.get(db);
            log.trace("Starting database reader with configuration: ");
            log.trace("host: {}", host);
            log.trace("port: {}", port);
            log.trace("db: {}", db);
            log.trace("start: {}", timeOffset);
            DatabaseReader reader = new DatabaseReader(host, port, db, start, messages);
            new Thread(reader).start();
        }
    }

}
