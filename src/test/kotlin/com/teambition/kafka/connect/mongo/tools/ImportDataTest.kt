package com.teambition.kafka.connect.mongo.tools

import com.google.common.truth.Truth.assertThat
import com.mongodb.client.MongoDatabase
import com.teambition.kafka.connect.mongo.utils.Mongod
import info.batey.kafka.unit.KafkaUnit
import org.apache.commons.lang.RandomStringUtils
import org.bson.Document
import org.json.JSONObject
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileInputStream
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
    private var db: MongoDatabase? = null

    @Before
    fun setUp() {
        db = mongod.start().getDatabase(dbName)
    }

    @After
    fun teardown() {
        mongod.stop()
    }

    @Test
    fun importDB() {
        val count = Math.max(Random().nextInt(200), 100)
        val collectionName = "users"
        bulkInsert(count, collectionName)
        val messages = ConcurrentLinkedQueue<MessageData>()

        val importDb = ImportDB(
            uri = "mongodb://localhost:12345",
            dbName = "$dbName.$collectionName",
            topicPrefix = "import_test",
            messages = messages)
        importDb.run()

        assertThat(messages.count()).isEqualTo(count)
    }

    /**
     * Test read config properties and initialize kafka producer
     */
    @Test
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

        cats.forEach { cat ->
            log.debug("Cat record: {}", cat)
            val key = JSONObject(cat.key()).toMap()
            val message = JSONObject(cat.message()).toMap()
            val keySchema = key["schema"] as Map<*, *>
            val keyPayload = key["payload"] as String
            val messageSchema = message["schema"] as Map<*, *>
            val messagePayload = message["payload"] as Map<*, *>
            val fields = (messageSchema["fields"] as List<*>)
                .map { it as Map<*, *> }
                .map { it["field"] }

            assertThat(cat.topic()).isEqualTo("import_test_${dbName}_cats")
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
    private fun bulkInsert(count: Int, collectionName: String) {
        val documents = mutableListOf<Document>()
        val db = db!!
        db.createCollection(collectionName)
        val collection = db.getCollection(collectionName)!!
        for (i in 0 until count) {
            documents.add(Document().append(
                RandomStringUtils.random(Random().nextInt(100), true, false),
                Random().nextInt()
            ))
        }
        collection.insertMany(documents)
    }
}
