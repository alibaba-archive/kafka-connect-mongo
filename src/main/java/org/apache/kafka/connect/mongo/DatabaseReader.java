package org.apache.kafka.connect.mongo;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tail oplog for one db
 *
 * @author Xu Jingxin
 */
public class DatabaseReader implements Runnable {

    private Logger log = LoggerFactory.getLogger(DatabaseReader.class);

    private String host;
    private Integer port;
    private String db;
    private String start;

    private ConcurrentLinkedQueue<Document> messages;
    private MongoCollection<Document> oplog;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private Bson query;

    /**
     * Connect and tail wait oplog
     * @param host 127.0.0.1
     * @param port 27017
     * @param db mydb.test
     * @param start timestamp.inc
     * @param messages
     */
    public DatabaseReader(String host,
                          Integer port,
                          String db,
                          String start,
                          ConcurrentLinkedQueue<Document> messages) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.start = start;
        this.messages = messages;

        mongoClient = new MongoClient(host, port);
        mongoDatabase = mongoClient.getDatabase("local");
        oplog = mongoDatabase.getCollection("oplog.rs");

        createQuery();

        log.trace("Start from {}", start);
    }

    @Override
    public void run() {
        FindIterable<Document> documents = oplog
                .find(query)
                .sort(new Document("$natural", 1))
                .projection(Projections.include("ts", "op", "ns", "o"))
                .cursorType(CursorType.TailableAwait);
        try {
            for (Document document : documents) {
                log.trace(document.toString());
                messages.add(document);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Closed connection");
        }
    }

    private MongoCollection connectOplog() {
        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase("local");
        oplog = db.getCollection("oplog.rs");
        return oplog;
    }

    private Bson createQuery() {
        Long timestamp = Long.parseLong(start.split(",")[0]);
        Long inc = Long.parseLong(start.split(",")[1]);

        query = Filters.and(
                Filters.exists("fromMigrate", false),
                Filters.gt("ts", new BsonTimestamp(timestamp.intValue(), inc.intValue())),
                Filters.or(
                        Filters.eq("op", "i"),
                        Filters.eq("op", "u"),
                        Filters.eq("op", "d")
                ),
                Filters.eq("ns", db)
        );

        return query;
    }

}
