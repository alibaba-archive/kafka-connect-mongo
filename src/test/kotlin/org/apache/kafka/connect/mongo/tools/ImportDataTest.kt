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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

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
        val messages = ConcurrentLinkedQueue<Map<String, String>>()
        val bulkSize = 10

        val importDb = ImportDB("mongodb://localhost:12345", "$dbName.$collectionName", "import_test", messages, bulkSize)
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
        // Import 30 hundreds messages from two collections
        importJob.start()

        // Read messages from topics
        kafkaUnit.readMessages("import_test_${dbName}_dogs", 20)
        val cats = kafkaUnit.readKeyedMessages("import_test_${dbName}_cats", 10)

        cats.forEach {
            log.debug("Cat record: {}", it)
            val key = JSONObject(it.key()).toMap()
            val message = JSONObject(it.message()).toMap()
            val keySchema = key["schema"] as Map<*, *>
            val keyPayload = key["payload"] as String
            val messageSchema = message["schema"] as Map<*, *>
            val messagePayload = message["payload"] as Map<*, *>
            val fields = (messageSchema["fields"] as List<Map<*, *>>).map {
                it["field"]
            }

            assertThat(it.topic()).isEqualTo("import_test_${dbName}_cats")
            assertThat(keySchema).isEqualTo(mapOf(
                    "type" to "string",
                    "optional" to true
            ))
            assertThat(keyPayload).matches("^[0-9a-z]{24}$".toRegex(RegexOption.IGNORE_CASE).toPattern())
            assertThat(messageSchema["type"]).isEqualTo("struct")
            assertThat(messageSchema["optional"]).isEqualTo(false)
            assertThat(messagePayload["database"]).isEqualTo("test_cats")
            assertThat(fields).containsAllOf("id", "database", "ts", "op", "inc", "object")
            assertThat(messagePayload.keys).containsAllOf("id", "database", "ts", "op", "inc", "object")
        }

        kafkaUnit.shutdown()
    }

    /**
     * Bulk insert random document into collection
     * @param count Count of messages
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