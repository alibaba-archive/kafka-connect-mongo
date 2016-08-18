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
    private Map<String, String> start;

    ConcurrentLinkedQueue<Document> messages;

    public MongoReader(String host,
                       Integer port,
                       List<String> dbs,
                       Map<String, String> start) {
        this.host = host;
        this.port = port;
        this.dbs = dbs;
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        for (String db: dbs) {
            String timeOffset = this.start.get(db);
            if (timeOffset == null) timeOffset = "0.0";
            log.trace("Starting database reader with configuration: ");
            log.trace("host: {}", host);
            log.trace("port: {}", port);
            log.trace("db: {}", db);
            log.trace("start: {}", timeOffset);
            DatabaseReader reader = new DatabaseReader(host, port, db, timeOffset, messages);
            new Thread(reader).start();
        }
    }

}
