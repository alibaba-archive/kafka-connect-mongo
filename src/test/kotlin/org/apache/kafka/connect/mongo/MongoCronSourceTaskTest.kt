package org.apache.kafka.connect.mongo

import com.google.common.truth.Truth.assertThat
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.mongo.utils.Mongod
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.bson.Document
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.powermock.api.easymock.PowerMock
import java.util.*

/**
 * @author Xu Jingxin
 */
class MongoCronSourceTaskTest {
    private val mongod = Mongod()
    private var task: MongoCronSourceTask? = null
    private val collections = listOf("c1", "c2")

    @Before
    fun setUp() {
        val db = mongod.start().getDatabase("mydb")
        collections.forEach { db.createCollection(it) }
        task = MongoCronSourceTask()
        val context = PowerMock.createMock(SourceTaskContext::class.java)
        task!!.initialize(context)
    }

    @After
    fun tearDown() {
        mongod.stop()
    }

    @Test
    fun start() {
        PowerMock.replayAll()
        mockData()
        var second = Calendar.getInstance().get(Calendar.SECOND) + 2
        if (second > 59) second = 0
        val props = mapOf(
                "mongo.uri" to "mongodb://localhost:12345",
                "batch.size" to "20",
                "schema.name" to "schema",
                "topic.prefix" to "prefix",
                "databases" to "mydb.c1,mydb.c2",
                "schedule" to "$second * * * * ?"
        )
        task!!.start(props)

        Thread.sleep(2000)
        val records = mutableListOf<SourceRecord>()
        var batchCount = 0
        do {
            val bulkRecords = task!!.poll()
            records.addAll(bulkRecords)
            batchCount += 1
        } while (bulkRecords.isNotEmpty())

        assertThat(records.size).isEqualTo(100)
        assertThat(batchCount - 1).isEqualTo(100 / 20)
        records.forEach {
            assertThat(it.key()).isInstanceOf(String::class.java)
            val value = it.value() as Struct
            assertThat(value.get("id")).isEqualTo(it.key())
            assertThat(value.get("op")).isEqualTo("i")
            assertThat(value.get("object")).isNotNull()
        }

        PowerMock.verifyAll()
    }

    private fun mockData() {
        val db = mongod.getDatabase("mydb")
        val docs = mutableMapOf<String, List<Document>>()
        (0..100 - 1).forEach {
            val doc = Document().append("n", it)
            val collectionName = collections[Random().nextInt(2)]
            if (docs[collectionName] == null) {
                docs[collectionName] = mutableListOf<Document>()
            }
            val docList = docs[collectionName] as MutableList<Document>
            docList.add(doc)
        }
        docs.forEach {
            val collection = db.getCollection(it.key)
            collection.insertMany(it.value)
        }
    }
}
