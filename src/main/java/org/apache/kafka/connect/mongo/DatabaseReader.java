package org.apache.kafka.connect.mongo;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
                .projection(Projections.include("ts", "op", "ns", "o", "o2"))
                .cursorType(CursorType.TailableAwait);
        try {
            for (Document document : documents) {
                log.trace(document.toString());
                document = handleOp(document);
                if (document != null) messages.add(document);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Closed connection");
        }
    }

    /**
     * Handle operations
     * i: keep oplog
     * u: find origin document
     * d: keep oplog
     * @param doc oplog
     * @return Document
     */
    private Document handleOp(Document doc) {
        switch ((String) doc.get("op")) {
            case "u":
                Document updated = findOneById(doc);
                if (updated == null) return null;
                doc.append("o", updated);
                break;
            default: break;
        }
        return doc;
    }

    private Document findOneById(Document doc) {
        try {
            String[] db = String.valueOf(doc.get("ns")).split("\\.");

            MongoDatabase nsDB = mongoClient.getDatabase(db[0]);
            MongoCollection<Document> nsCollection = nsDB.getCollection(db[1]);
            ObjectId _id = (ObjectId) ((Document) doc.get("o2")).get("_id");

            List<Document> docs = nsCollection.find(Filters.eq("_id", _id)).into(new ArrayList<>());

            return docs.get(0);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Can not find document: {}", doc);
        }
        return null;
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
