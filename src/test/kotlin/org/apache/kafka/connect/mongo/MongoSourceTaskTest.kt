package org.apache.kafka.connect.mongo

import com.mongodb.BasicDBObject
import com.mongodb.client.model.Filters
import com.mongodb.util.JSON
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.mongo.utils.Mongod
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
import java.util.*
import kotlin.collections.HashMap

/**
 * @author Xu Jingxin
 */
class MongoSourceTaskTest {

    companion object {
        private val log = LoggerFactory.getLogger(MongoSourceTaskTest::class.java)
        private val collections = Mongod.collections
        private val mydb = "mydb"
    }

    private var task: MongoSourceTask? = null
    private var sourceTaskContext: SourceTaskContext? = null
    private var offsetStorageReader: OffsetStorageReader? = null
    private var sourceProperties = mutableMapOf<String, String>()
    private val mongod = Mongod()

    @Before
    fun setUp() {
        val db = mongod.start().getDatabase(mydb)
        collections.forEach { db.createCollection(it) }

        task = MongoSourceTask()
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader::class.java)
        sourceTaskContext = PowerMock.createMock(SourceTaskContext::class.java)
        task!!.initialize(sourceTaskContext)

        sourceProperties.put("mongo.uri", "mongodb://localhost:12345")
        sourceProperties.put("initial.import", "true")
        sourceProperties.put("batch.size", "20")
        sourceProperties.put("schema.name", "schema")
        sourceProperties.put("topic.prefix", "prefix")
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3")
    }

    @After
    fun tearDown() {
        mongod.stop()
    }

    @Test
    fun pollWithNullOffset() {
        expectOffsetLookupReturnNull()
        PowerMock.replayAll()

        testInitialWhenStart()
        testBulkInsert()
        testSubtleInsert()

        PowerMock.verifyAll()
    }

    @Test
    fun pollWithOffset() {
        expectOffsetLookupReturnOffset()
        PowerMock.replayAll()

        testInitialWhenStart()
        testBulkInsert()
        testSubtleInsert()

        PowerMock.verifyAll()
    }

    private fun expectOffsetLookupReturnNull() {
        expect(sourceTaskContext!!.offsetStorageReader()).andReturn(offsetStorageReader).anyTimes()
        expect(offsetStorageReader!!.offset(EasyMock.anyObject<Map<String, String>>())).andReturn(HashMap<String, Any>()).anyTimes()
    }

    private fun expectOffsetLookupReturnOffset() {
        expect(sourceTaskContext!!.offsetStorageReader()).andReturn(offsetStorageReader).anyTimes()
        for (collection in collections) {
            val timestamp = BsonTimestamp(Math.floor((System.currentTimeMillis() / 1000).toDouble()).toInt(), 0)
            expect(offsetStorageReader!!.offset(Collections.singletonMap("mongo", "mydb." + collection)))
                .andReturn(Collections.singletonMap<String, Any>("mydb." + collection, timestamp.time.toString() + ",0"))
                .anyTimes()
        }
    }

    /**
     * Insert documents on random collections
     */
    private fun bulkInsert(totalNumber: Int) {
        val db = mongod.getDatabase(mydb)
        for (i in 0..totalNumber - 1) {
            val newDocument = Document().append(RandomStringUtils.random(Random().nextInt(100), true, false), Random().nextInt())
            db.getCollection(collections[Random().nextInt(3)]).insertOne(newDocument)
        }
        // Ensure messages are inserted
        Thread.sleep(1000)
    }

    /**
     * Some predefined operations on collection 2
     * Two insert
     * One update
     * One delete
     */
    private fun subtleInsert() {
        val db = mongod.getDatabase(mydb)
        val doc1 = Document().append("text", "doc1")
        val doc2 = Document().append("text", "doc2")

        val test1 = db.getCollection(collections[0])
        test1.insertOne(doc1)
        test1.insertOne(doc2)
        test1.updateOne(Filters.eq("text", "doc1"),
            Document("\$set", Document("name", "Stephen")))
        test1.deleteOne(Filters.eq("text", "doc2"))
        // Ensure messages are inserted
        Thread.sleep(1000)
    }

    private fun testBulkInsert() {
        // Insert an amount of documents
        // Check for the received count
        val totalCount = Math.max(Random().nextInt(200), 101)
        log.debug("Bulk insert count: {}", totalCount)
        bulkInsert(totalCount)

        val records = ArrayList<SourceRecord>()
        var pollRecords: List<SourceRecord>
        do {
            pollRecords = task!!.poll()
            records.addAll(pollRecords)
        } while (!pollRecords.isEmpty())
        log.debug("Record size: {}", records.size)
        // records contains all the collection documents and oplog documents
        val disDitinctRecords = records.distinctBy { (it.value() as Struct).get("id") }
        assertEquals(totalCount.toLong(), disDitinctRecords.size.toLong())
    }

    private fun testInitialWhenStart() {
        // Insert an amount of documents
        // Check for the received count
        val totalCount = Math.max(Random().nextInt(200), 101)
        log.debug("Bulk insert count: {}", totalCount)
        bulkInsert(totalCount)
        task!!.start(sourceProperties)

        Thread.sleep(2000)
        val records = ArrayList<SourceRecord>()
        var pollRecords: List<SourceRecord>
        do {
            pollRecords = task!!.poll()
            records.addAll(pollRecords)
        } while (!pollRecords.isEmpty())
        log.debug("Record size: {}", records.size)
        // records contains all the collection documents and oplog documents
        val disDitinctRecords = records.distinctBy { (it.value() as Struct).get("id") }
        assertEquals(totalCount.toLong(), disDitinctRecords.size.toLong())
    }

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

        val values = ArrayList<Struct>()
        val keys = ArrayList<String>()
        records.forEach { record ->
            values.add(record.value() as Struct)
            if (record.key() != null) {
                keys.add(record.key() as String)
            }
        }

        // Test struct of each record
        assertEquals(keys.count(), 4)
        assertNotEquals(values[0].get("id"), null)
        assertNotEquals(values[1].get("id"), null)
        assertEquals(values[0].get("id"), values[2].get("id"))
        assertEquals(values[1].get("id"), values[3].get("id"))
        assertEquals(values[0].get("op"), "i")
        assertEquals(values[2].get("op"), "u")
        assertEquals(values[3].get("op"), "d")

        val updatedValue = values[2].get("object") as String
        val updatedObject = JSON.parse(updatedValue) as BasicDBObject

        assertEquals("Stephen", updatedObject.get("name"))

        assertEquals(null, values[3].get("object"))
    }
}
