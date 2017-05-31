package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.mongo.utils.Mongod
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */

class DatabaseReaderTest {
    private val mongod = Mongod()
    private var reader: DatabaseReader? = null

    @Before
    fun setUp() {
        val db = mongod.start().getDatabase("mydb")
        db.createCollection("test")
    }

    @After
    fun tearDown() {
        mongod.stop()
    }

    @Test
    fun connectWithPassword() {
        mongod.createUserWithPassword()

        val messages = ConcurrentLinkedQueue<Document>()
        reader = DatabaseReader("mongodb://test:123456@localhost:12345",
                "mydb.test",
                "0,0",
                messages)
        Thread(reader).start()

        // Wait for database reader connection client
        Thread.sleep(500)

        val collection = mongod.getDatabase("mydb").getCollection("test")
        for (i in 0..100) {
            collection.insertOne(Document().append("name", "Eric$i"))
        }

        messages.poll() ?: throw Exception("Can not read messages")
    }
}
