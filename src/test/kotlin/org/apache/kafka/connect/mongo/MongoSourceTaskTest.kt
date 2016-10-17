package org.apache.kafka.connect.mongo

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.client.model.Filters
import com.mongodb.util.JSON
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.IMongodConfig
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.config.Storage
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.bson.BsonTimestamp
import org.bson.Document
import org.easymock.EasyMock
import org.easymock.EasyMock.expect
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Before
import org.junit.Test
import org.powermock.api.easymock.PowerMock
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

/**
 * Created by Xu Jingxin on 16/8/16.
 */
class MongoSourceTaskTest {

    companion object {
        private val log = LoggerFactory.getLogger(MongoSourceTaskTest::class.java)
        private val REPLICATION_PATH = "tmp"
    }

    private var task: MongoSourceTask? = null
    private var sourceTaskContext: SourceTaskContext? = null
    private var offsetStorageReader: OffsetStorageReader? = null
    private var sourceProperties: MutableMap<String, String>? = null

    private var mongodExecutable: MongodExecutable? = null
    private var mongodProcess: MongodProcess? = null
    private var mongodStarter: MongodStarter? = null
    private var mongodConfig: IMongodConfig? = null
    private var mongoClient: MongoClient? = null

    private val collections = object : ArrayList<String>() {
        init {
            add("test1")
            add("test2")
            add("test3")
        }
    }

    @Before
    @Throws(Exception::class)
    fun setUp() {
        try {
            startMongo()
        } catch (e: Exception) {
            e.printStackTrace()
        }

        task = MongoSourceTask()
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader::class.java)
        sourceTaskContext = PowerMock.createMock(SourceTaskContext::class.java)
        task!!.initialize(sourceTaskContext)

        sourceProperties = HashMap<String, String>()
        sourceProperties!!.put("host", "localhost")
        sourceProperties!!.put("port", "12345")
        sourceProperties!!.put("batch.size", "20")
        sourceProperties!!.put("schema.name", "schema")
        sourceProperties!!.put("topic.prefix", "prefix")
        sourceProperties!!.put("databases", "mydb.test1,mydb.test2,mydb.test3")
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        mongodProcess!!.stop()
        mongodExecutable!!.stop()
        FileUtils.deleteDirectory(File(REPLICATION_PATH))
    }

    @Test
    @Throws(Exception::class)
    fun pollWithNullOffset() {
        expectOffsetLookupReturnNull()
        PowerMock.replayAll()

        task!!.start(sourceProperties!!)
        testBulkInsert()
        testSubtleInsert()

        PowerMock.verifyAll()
    }

    @Test
    @Throws(Exception::class)
    fun pollWithOffset() {
        expectOffsetLookupReturnOffset()
        PowerMock.replayAll()

        task!!.start(sourceProperties!!)
        testBulkInsert()
        testSubtleInsert()

        PowerMock.verifyAll()
    }

    @Throws(Exception::class)
    private fun startMongo() {
        mongodStarter = MongodStarter.getDefaultInstance()
        mongodConfig = MongodConfigBuilder().version(Version.Main.V3_3).replication(Storage(REPLICATION_PATH, "rs0", 1024)).net(Net(12345, Network.localhostIsIPv6())).build()
        mongodExecutable = mongodStarter!!.prepare(mongodConfig!!)
        mongodProcess = mongodExecutable!!.start()
        mongoClient = MongoClient(ServerAddress("localhost", 12345))
        val adminDatabase = mongoClient!!.getDatabase("admin")
        val replicaSetSetting = BasicDBObject()
        replicaSetSetting.put("_id", "rs0")
        val members = BasicDBList()
        val host = BasicDBObject()
        host.put("_id", 0)
        host.put("host", "127.0.0.1:12345")
        members.add(host)
        replicaSetSetting.put("members", members)
        adminDatabase.runCommand(BasicDBObject("isMaster", 1))
        adminDatabase.runCommand(BasicDBObject("replSetInitiate", replicaSetSetting))
        val db = mongoClient!!.getDatabase("mydb")
        collections.forEach { db.createCollection(it) }
    }

    private fun expectOffsetLookupReturnNull() {
        expect(sourceTaskContext!!.offsetStorageReader()).andReturn(offsetStorageReader)
        expect(offsetStorageReader!!.offsets(EasyMock.anyObject<List<Map<String, String>>>())).andReturn(HashMap<Map<String, String>, Map<String, Any>>())
    }

    private fun expectOffsetLookupReturnOffset() {
        val offsetMap = HashMap<Map<String, String>, Map<String, Any>>()
        for (collection in collections) {
            val timestamp = BsonTimestamp(Math.floor((System.currentTimeMillis() / 1000).toDouble()).toInt(), 0)
            offsetMap.put(
                    MongoSourceTask.getPartition("mydb." + collection),
                    Collections.singletonMap<String, Any>("mydb." + collection, timestamp.time.toString() + ",0"))
        }
        log.debug("Offsets: {}", offsetMap)
        expect(sourceTaskContext!!.offsetStorageReader()).andReturn(offsetStorageReader)
        expect(offsetStorageReader!!.offsets(EasyMock.anyObject<List<Map<String, String>>>())).andReturn(offsetMap)
    }

    /**
     * Insert documents on random collections
     */
    private fun bulkInsert(totalNumber: Int) {
        val db = mongoClient!!.getDatabase("mydb")
        for (i in 0..totalNumber - 1) {
            val newDocument = Document().append(RandomStringUtils.random(Random().nextInt(100), true, false), Random().nextInt())
            db.getCollection(collections[Random().nextInt(3)]).insertOne(newDocument)
        }
    }

    /**
     * Some predefined operations on collection 2
     * Two insert
     * One update
     * One delete
     */
    private fun subtleInsert() {
        val db = mongoClient!!.getDatabase("mydb")
        val doc1 = Document().append("text", "doc1")
        val doc2 = Document().append("text", "doc2")

        val test1 = db.getCollection(collections[0])
        test1.insertOne(doc1)
        test1.insertOne(doc2)
        test1.updateOne(Filters.eq("text", "doc1"),
                Document("\$set", Document("name", "Stephen")))
        test1.deleteOne(Filters.eq("text", "doc2"))
    }

    @Throws(InterruptedException::class)
    private fun testBulkInsert() {
        // Insert an amount of documents
        // Check for the received count
        val totalCount = Math.max(Random().nextInt(200), 100)
        log.debug("Bulk insert count: {}", totalCount)
        bulkInsert(totalCount)

        val records = ArrayList<SourceRecord>()
        var pollRecords: List<SourceRecord>
        do {
            pollRecords = task!!.poll()
            records.addAll(pollRecords)
        } while (!pollRecords.isEmpty())
        log.debug("Record size: {}", records.size)
        assertEquals(totalCount.toLong(), records.size.toLong())
    }

    @Throws(InterruptedException::class)
    private fun testSubtleInsert() {
        // Insert some pre defined actions
        // Check for the document structure
        log.debug("Subtle insert")
        subtleInsert()

        val records = ArrayList<SourceRecord>()
        var pollRecords: List<SourceRecord>

        do {
            pollRecords = task!!.poll()
            records.addAll(pollRecords)
        } while (!pollRecords.isEmpty())

        log.debug("Subtle records: {}", records)

        assertEquals(4, records.size.toLong())

        val structs = ArrayList<Struct>()
        records.forEach { record -> structs.add(record.value() as Struct) }

        // Test struct of each record
        assertNotEquals(structs[0].get("id"), null)
        assertNotEquals(structs[1].get("id"), null)
        assertEquals(structs[0].get("id"), structs[2].get("id"))
        assertEquals(structs[1].get("id"), structs[3].get("id"))

        val updatedValue = structs[2].get("object") as String
        val updatedObject = JSON.parse(updatedValue) as BasicDBObject

        assertEquals("Stephen", updatedObject.get("name"))

        assertEquals(null, structs[3].get("object"))
    }
}