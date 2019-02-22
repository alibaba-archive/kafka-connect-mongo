package com.teambition.kafka.connect.mongo.sink

import com.google.common.truth.Truth.assertThat
import com.teambition.kafka.connect.mongo.database.MongoClientLoader
import com.teambition.kafka.connect.mongo.utils.Mongod
import org.apache.commons.lang.RandomStringUtils
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.powermock.api.easymock.PowerMock
import java.util.*

/**
 * @author Xu Jingxin
 */
class MongoSinkTaskTest {
    private val mongod = Mongod()
    private var task: MongoSinkTask? = null
    private var taskContext: SinkTaskContext? = null
    private val keySchema = Schema.OPTIONAL_STRING_SCHEMA
    private val valueSchema = SchemaBuilder.struct()
        .field("ts", Schema.OPTIONAL_INT32_SCHEMA)
        .field("inc", Schema.OPTIONAL_INT32_SCHEMA)
        .field("id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("database", Schema.OPTIONAL_STRING_SCHEMA)
        .field("op", Schema.OPTIONAL_STRING_SCHEMA)
        .field("object", Schema.OPTIONAL_STRING_SCHEMA).build()
    private var offset = 0L

    @Before
    fun setUp() {
        task = MongoSinkTask()
        taskContext = PowerMock.createMock(SinkTaskContext::class.java)
        task!!.initialize(taskContext)

        mongod.start()

        MongoClientLoader.getClient("mongodb://localhost:12345", reconnect = true)
    }

    @After
    fun tearDown() {
        mongod.stop()
    }

    @Test
    fun putBulk() {
        PowerMock.replayAll()
        val topics = listOf("a", "b")
        val props = mapOf(
            "mongo.uri" to "mongodb://localhost:12345",
            "topics" to topics.joinToString(","),
            "databases" to "t.a,t.b"
        )
        task!!.start(props)

        // Mock records
        for (i in 1..10) {
            val recordsMap = mutableMapOf(
                "a" to mutableListOf<SinkRecord>(),
                "b" to mutableListOf()
            )
            for (n in 1..10) {
                val topic = topics[Random().nextInt(2)]
                val sinkRecord = createRecord(topic, "i")
                recordsMap[topic]!!.add(sinkRecord)
            }
            recordsMap.forEach {
                task!!.put(it.value)
            }
        }

        // Verify messages in mongodb
        val documents = mongod.getDatabase("t").getCollection("a").find()
        documents.forEach {
            assertThat(it.keys).containsAllOf("_id", "state")
            assertThat(it["state"]).isInstanceOf(Int::class.javaObjectType)
        }
        // Verify document count
        assertThat(countAll(topics)).isEqualTo(100)

        PowerMock.verifyAll()
    }

    @Test
    fun putSubtle() {
        PowerMock.replayAll()
        val topic = "a"
        val props = mapOf(
            "mongo.uri" to "mongodb://localhost:12345",
            "topics" to topic,
            "databases" to "t.a"
        )
        task!!.start(props)
        // Mock records
        val r1 = createRecord(topic, "i")
        val r2 = createRecord(topic, "i")
        val r3 = updateRecord(r1)
        val r4 = deleteRecord(r2)

        task!!.put(listOf(r1, r2, r3, r4))

        val documents = mongod.getDatabase("t").getCollection("a").find()
        assertThat(documents.count()).isEqualTo(1)
        val doc1 = documents.first()
        assertThat(doc1["_id"].toString()).isEqualTo(r1.key())
        assertThat(doc1["state"]).isEqualTo(-1)

        PowerMock.verifyAll()
    }

    private fun countAll(topics: List<String>): Int {
        val db = mongod.getDatabase("t")
        return topics.sumBy {
            db.getCollection(it).count().toInt()
        }
    }

    /**
     * Mock new record
     */
    private fun createRecord(topic: String, op: String): SinkRecord {
        val id = ObjectId()
        val doc = Document()
            .append("_id", id)
            .append("state", Random().nextInt())
        val message = Struct(valueSchema)
            .put("id", id.toHexString())
            .put("ts", id.timestamp)
            .put("inc", 0)
            .put("op", op)
            .put("database", "t_$topic")
            .put("object", doc.toJson())
        return SinkRecord(
            topic,
            0,
            keySchema,
            message["id"],
            valueSchema,
            message,
            ++offset
        )
    }

    /**
     * Mock a update record
     */
    private fun updateRecord(record: SinkRecord): SinkRecord {
        val _id = ObjectId(record.key() as String)
        // Modify the state key
        val doc = Document()
            .append("_id", _id)
            .append("state", -1)
            .append(RandomStringUtils.random(Random().nextInt(100), true, false), Random().nextInt())
        val message = Struct(valueSchema)
            .put("id", _id.toHexString())
            .put("ts", _id.timestamp)
            .put("inc", 1)
            .put("op", "u")
            .put("database", "t_${record.topic()}")
            .put("object", doc.toJson())
        return SinkRecord(
            record.topic(),
            0,
            keySchema,
            message["id"],
            valueSchema,
            message,
            ++offset
        )
    }

    /**
     * Mock a delete record
     */
    private fun deleteRecord(record: SinkRecord): SinkRecord {
        var message = record.value() as Struct
        val id = message["id"]
        message = Struct(valueSchema)
            .put("id", id)
            .put("ts", message["ts"])
            .put("inc", 0)
            .put("op", "d")
            .put("database", message["database"])
        return SinkRecord(
            record.topic(),
            0,
            keySchema,
            id,
            valueSchema,
            message,
            ++offset
        )
    }
}
