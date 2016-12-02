package org.apache.kafka.connect.mongo.tools

import com.mongodb.client.MongoDatabase
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.connect.mongo.utils.Mongod
import info.batey.kafka.unit.KafkaUnit
import org.bson.Document
import org.json.JSONObject
import org.junit.After
import org.slf4j.LoggerFactory
import org.junit.Test
import org.junit.Before
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import com.google.common.truth.Truth.assertThat

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
        val messages = ConcurrentLinkedQueue<JSONObject>()

        val importDb = ImportDB("mongodb://localhost:12345", "$dbName.$collectionName", messages)
        importDb.run()

        assertThat(messages.count()).isEqualTo(recordsCount)
    }

    /**
     * Test read config properties and initialize kafka producer
     */
    @Test
    @Throws(IOException::class)
    fun startJob() {
        val kafkaUnit = KafkaUnit(5000, 5001)
        kafkaUnit.startup()

        val basePath = File(".").canonicalPath
        val props = Properties()
        props.load(FileInputStream("$basePath/src/test/resources/producer.properties"))

        val importJob = ImportJob("mongodb://localhost:12345", "$dbName.cats,$dbName.dogs", "import_test", props)

        bulkInsert(20, "dogs")
        bulkInsert(10, "cats")
        // Import 30 hundreds documents from two collections
        importJob.start()

        // Read messages from topics
        kafkaUnit.readMessages("import_test_${dbName}_dogs", 20)
        val cats = kafkaUnit.readMessages("import_test_${dbName}_cats", 10)

        cats.forEach {
            val message = JSONObject(it)
            assertThat(message["database"]).isEqualTo("test_cats")
            assertThat(message.keySet()).containsAllOf("id", "database", "ts", "inc", "object")
        }

        kafkaUnit.shutdown()
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