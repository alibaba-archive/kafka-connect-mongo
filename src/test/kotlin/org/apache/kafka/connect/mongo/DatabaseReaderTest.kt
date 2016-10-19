package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.mongo.utils.Mongod
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */

class DatabaseReaderTest {
    companion object {
        val log = LoggerFactory.getLogger(DatabaseReaderTest::class.java)
    }
    private val mongod = Mongod()
    private var reader : DatabaseReader? = null
    private val messages = ConcurrentLinkedQueue<Document>()

    @Before
    @Throws(Exception::class)
    fun setUp() {
        val db = mongod.start().getDatabase("mydb")!!
        db.createCollection("test")

        reader = DatabaseReader("localhost",
                12345,
                "mydb.test",
                "0,0",
                messages)
        Thread(reader).start()

        val collection = db.getCollection("test")
        for (i in 0..100) {
            collection.insertOne(Document().append("name", "Eric$i"))
        }
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        mongod.stop()
    }

    @Test
    @Throws(Exception::class)
    fun connectWithPassword() {
        messages.poll() ?: throw Exception("Can not read messages")
    }
}