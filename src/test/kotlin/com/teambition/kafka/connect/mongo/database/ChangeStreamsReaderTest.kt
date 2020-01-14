package com.teambition.kafka.connect.mongo.database

import com.google.common.truth.Truth.assertThat
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import com.teambition.kafka.connect.mongo.utils.Mongod
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.concurrent.thread

/**
 * @author Xu Jingxin
 */
class ChangeStreamsReaderTest {

    private val mongod = Mongod()

    @Before
    fun setUp() {
        val db = mongod.start().getDatabase("mydb")
        db.createCollection("mycoll")
    }

    @After
    fun tearDown() {
        mongod.stop()
    }

//    @Test
//    fun run() {
//        val messages = ConcurrentLinkedQueue<Document>()
//        val offset = MongoSourceOffset(null)
//        thread { ChangeStreamsReader(mongod.uri, "mydb.mycoll", offset, messages).run() }
//        Thread.sleep(1000)
//        val collection = mongod.getDatabase("mydb").getCollection("mycoll")
//        for (i in 1..3) {
//            collection.insertOne(Document())
//        }
//        Thread.sleep(1000)
//        assertThat(messages).hasSize(3)
//    }

}

