package org.apache.kafka.connect.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.*;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.expect;

/**
 * Created by Xu Jingxin on 16/8/16.
 */
public class MongoSourceTaskTest {

    private final static Logger log = LoggerFactory.getLogger(MongoSourceTaskTest.class);

    private static String REPLICATION_PATH = "/tmp/mongo";

    private MongoSourceTask task;
    private SourceTaskContext sourceTaskContext;
    private OffsetStorageReader offsetStorageReader;
    private Map<String, String> sourceProperties;

    private MongodExecutable mongodExecutable;
    private MongodProcess mongodProcess;
    private MongodStarter mongodStarter;
    private IMongodConfig mongodConfig;
    private MongoClient mongoClient;

    private List<String> collections = new ArrayList<String>() {{
        add("test1");
        add("test2");
        add("test3");
    }};

    @Before
    public void setUp() throws Exception {
        try {
            startMongo();
        } catch (Exception e) {
            e.printStackTrace();
        }

        task = new MongoSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        sourceTaskContext = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(sourceTaskContext);

        sourceProperties = new HashMap<>();
        sourceProperties.put("host", "localhost");
        sourceProperties.put("port", "12345");
        sourceProperties.put("batch.size", "20");
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic.prefix", "prefix");
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3");
    }

    @After
    public void tearDown() throws Exception {
        mongodProcess.stop();
        mongodExecutable.stop();
        FileUtils.deleteDirectory(new File(REPLICATION_PATH));
    }

    @Test
    public void pollWithNullOffset() throws Exception {
        expectOffsetLookupReturnNull();
        PowerMock.replayAll();

        task.start(sourceProperties);
        testBulkInsert();
        testSubtleInsert();

        PowerMock.verifyAll();
    }

    @Test
    public void pollWithOffset() throws Exception {
        expectOffsetLookupReturnOffset();
        PowerMock.replayAll();

        task.start(sourceProperties);
        testBulkInsert();
        testSubtleInsert();

        PowerMock.verifyAll();
    }

    private void startMongo() throws Exception {
        mongodStarter = MongodStarter.getDefaultInstance();
        mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.V3_3)
                .replication(new Storage(REPLICATION_PATH, "rs0", 1024))
                .net(new Net(12345, Network.localhostIsIPv6()))
                .build();
        mongodExecutable = mongodStarter.prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();
        mongoClient = new MongoClient(new ServerAddress("localhost", 12345));
        MongoDatabase adminDatabase = mongoClient.getDatabase("admin");
        BasicDBObject replicaSetSetting = new BasicDBObject();
        replicaSetSetting.put("_id", "rs0");
        BasicDBList members = new BasicDBList();
        DBObject host = new BasicDBObject();
        host.put("_id", 0);
        host.put("host", "127.0.0.1:12345");
        members.add(host);
        replicaSetSetting.put("members", members);
        adminDatabase.runCommand(new BasicDBObject("isMaster", 1));
        adminDatabase.runCommand(new BasicDBObject("replSetInitiate", replicaSetSetting));
        MongoDatabase db = mongoClient.getDatabase("mydb");
        collections.forEach(db::createCollection);
    }

    private void expectOffsetLookupReturnNull() {
        expect(sourceTaskContext.offsetStorageReader()).andReturn(offsetStorageReader);
        expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>> anyObject())).andReturn(new HashMap<>());
    }

    private void expectOffsetLookupReturnOffset() {
        Map<Map<String, String>, Map<String, Object>> offsetMap = new HashMap<>();
        for (String collection : collections) {
            BsonTimestamp timestamp = new BsonTimestamp((int) Math.floor(System.currentTimeMillis() / 1000), 0);
            offsetMap.put(
                    MongoSourceTask.getPartition("mydb." + collection),
                    Collections.singletonMap("mydb." + collection, String.valueOf(timestamp.getTime()) + ",0")
            );
        }
        log.debug("Offsets: {}", offsetMap);
        expect(sourceTaskContext.offsetStorageReader()).andReturn(offsetStorageReader);
        expect(offsetStorageReader.offsets(EasyMock.<List<Map<String, String>>> anyObject())).andReturn(offsetMap);
    }

    /**
     * Insert documents on random collections
     */
    private void bulkInsert(Integer totalNumber) {
        MongoDatabase db = mongoClient.getDatabase("mydb");
        for (int i = 0; i < totalNumber; i++) {
            Document newDocument = new Document()
                    .append(RandomStringUtils.random(new Random().nextInt(100), true, false), new Random().nextInt());
            db.getCollection(collections.get(new Random().nextInt(3))).insertOne(newDocument);
        }
    }

    /**
     * Some predefined operations on collection 2
     * Two insert
     * One update
     * One delete
     */
    private void subtleInsert() {
        MongoDatabase db = mongoClient.getDatabase("mydb");
        Document doc1 = new Document().append("text", "doc1");
        Document doc2 = new Document().append("text", "doc2");

        MongoCollection<Document> test1 = db.getCollection(collections.get(0));
        test1.insertOne(doc1);
        test1.insertOne(doc2);
        test1.updateOne(Filters.eq("text", "doc1"),
                new Document("$set", new Document("name", "Stephen")));
        test1.deleteOne(Filters.eq("text", "doc2"));
    }

    private void testBulkInsert() throws InterruptedException {
        // Insert an amount of documents
        // Check for the received count
        int totalCount = Math.max(new Random().nextInt(200), 100);
        log.debug("Bulk insert count: {}", totalCount);
        bulkInsert(totalCount);

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> pollRecords;
        do {
            pollRecords = task.poll();
            records.addAll(pollRecords);
        } while (!pollRecords.isEmpty());
        log.debug("Record size: {}", records.size());
        assertEquals(totalCount, records.size());
    }

    private void testSubtleInsert() throws InterruptedException {
        // Insert some pre defined actions
        // Check for the document structure
        log.debug("Subtle insert");
        subtleInsert();

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> pollRecords;

        do {
            pollRecords = task.poll();
            records.addAll(pollRecords);
        } while (!pollRecords.isEmpty());

        assertEquals(4, records.size());

        List<Struct> structs = new ArrayList<>();
        records.forEach((record) -> {
            structs.add((Struct) record.value());
        });

        // Test struct of each record
        assertEquals(structs.get(0).get("id"), structs.get(2).get("id"));
        assertEquals(structs.get(1).get("id"), structs.get(3).get("id"));

        String updatedValue = (String) structs.get(2).get("object");
        BasicDBObject updatedObject = (BasicDBObject) JSON.parse(updatedValue);

        assertEquals("Stephen", updatedObject.get("name"));
    }

}