package org.apache.kafka.connect.mongo.tools

import com.mongodb.client.MongoDatabase
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.connect.mongo.utils.Mongod
import org.bson.Document
import org.junit.After
import org.slf4j.LoggerFactory
import org.junit.Test
import org.junit.Assert.*
import org.junit.Before
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
class ImportDataTest {
    companion object {
        private val log = LoggerFactory.getLogger(ImportDataTest::class.java)
    }

    private val mongod = Mongod()
    private val dbName = "test"
    private var recordsCount = 0
    private var db: MongoDatabase? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = mongod.start().getDatabase(dbName)
    }

    @After
    @Throws(Exception::class)
    fun teardown() {
        mongod.stop()
    }

    @Test
    fun importDB() {
        recordsCount = Math.max(Random().nextInt(200), 100)
        val collectionName = "users"
        bulkInsert(recordsCount, collectionName)
        val messages = ConcurrentLinkedQueue<Document>()

        val importDb = ImportDB("mongodb://localhost:12345", "$dbName.$collectionName", messages)
        importDb.run()

        assertEquals(messages.count(), recordsCount)
    }

    /**
     * Test read config properties and initialize kafka producer
     */
    @Test
    @Throws(IOException::class)
    fun startJob() {
        val basePath = File(".").canonicalPath
        val props = Properties()
        props.load(FileInputStream("$basePath/src/test/resources/producer.properties"))

        bulkInsert(100, "cats")
        bulkInsert(200, "dogs")

        // Import 300 hundreds documents from two collections
        val importJob = ImportJob("mongodb://localhost:12345", "$dbName.cats,$dbName.dogs", "import_test", props)
        importJob.start()
    }

    /**
     * Bulk insert random document into collection
     * @param count Count of documents
     * @param collectionName Name of collection
     */
    fun bulkInsert(count: Int, collectionName: String) {
        val documents = mutableListOf<Document>()
        val db = db!!
        db.createCollection(collectionName)
        val collection = db.getCollection(collectionName)!!
        for (i in 0..count - 1) {
            documents.add(Document().append(
                    RandomStringUtils.random(Random().nextInt(100), true, false),
                    Random().nextInt()
            ))
        }
        collection.insertMany(documents)
    }
}