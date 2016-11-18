package org.apache.kafka.connect.mongo.tools

import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.connect.mongo.utils.Mongod
import org.apache.kafka.connect.mongo.tools.ImportDB
import org.bson.Document
import org.junit.After
import org.slf4j.LoggerFactory
import org.junit.Test

import org.junit.Assert.*
import org.junit.Before
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
class ImportDataTest {

    private val logger = LoggerFactory.getLogger(ImportDataTest::class.java)

    private val mongod = Mongod()
    private val dbName = "dbName"
    private val collectionName = "users"
    private var recordsCount = 0
    private var db: MongoDatabase? = null
    private var collection: MongoCollection<Document>? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = mongod.start().getDatabase(dbName)
        db!!.createCollection(collectionName)
        collection = db!!.getCollection(collectionName)
    }

    @After
    @Throws(Exception::class)
    fun teardown() {
        mongod.stop()
    }

    @Test
    fun run() {
        recordsCount = Math.max(Random().nextInt(200), 100)
        val documents = mutableListOf<Document>()
        for (i in 0..recordsCount - 1) {
            documents.add(Document().append(
                    RandomStringUtils.random(Random().nextInt(100), true, false),
                    Random().nextInt()))
        }
        collection!!.insertMany(documents)
        val messages = ConcurrentLinkedQueue<Document>()

        val importDb = ImportDB("mongodb://localhost:12345", "${dbName}.${collectionName}", messages)
        importDb.run()

        assertEquals(messages.count(), recordsCount)
    }
}