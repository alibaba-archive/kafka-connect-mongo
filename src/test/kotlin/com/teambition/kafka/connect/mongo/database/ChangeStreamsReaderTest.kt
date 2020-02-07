package com.teambition.kafka.connect.mongo.database

import com.google.common.truth.Truth.assertThat
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.model.Filters
import com.teambition.kafka.connect.mongo.source.Message
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import com.teambition.kafka.connect.mongo.utils.Mongod
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
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
    private lateinit var client: MongoClient
    private val mydb = "mydb"

    @Before
    fun setUp() {
        mongod.initialize()
        client = MongoClient(MongoClientURI(mongod.uri))
        val db = client.getDatabase(mydb)
        db.createCollection("mycoll")
    }

    @After
    fun tearDown() {
        client.dropDatabase(mydb)
        client.close()
    }

    @Test
    fun run() {
        val m1 = ConcurrentLinkedQueue<Message>()
        // Read from newest offset
        val offset = MongoSourceOffset()
        thread { ChangeStreamsReader(mongod.uri, "mydb.mycoll", offset, m1).run() }
        Thread.sleep(1000)
        val collection = client.getDatabase(mydb).getCollection("mycoll")
        // Check insert and update
        collection.insertOne(Document("action", 1))
        collection.updateOne(
            Filters.eq("action", 1),
            Document("\$set", Document("action", 2))
        )
        Thread.sleep(1000)
        assertThat(m1).hasSize(2)

        val insert = m1.elementAt(0)
        assertThat(insert.oplog["ts"]).isInstanceOf(BsonTimestamp::class.java)
        assertThat(insert.oplog["ns"]).isEqualTo("mydb.mycoll")
        assertThat(insert.oplog["op"]).isEqualTo("i")
        assertThat((insert.oplog["o"] as Document)["action"]).isEqualTo(1)

        val update = m1.elementAt(1)
        assertThat(update.oplog["ts"]).isInstanceOf(BsonTimestamp::class.java)
        assertThat(update.oplog["ns"]).isEqualTo("mydb.mycoll")
        assertThat(update.oplog["op"]).isEqualTo("u")
        assertThat((update.oplog["o"] as Document)["action"]).isEqualTo(2)

        // Read with last offset
        val m2 = ConcurrentLinkedQueue<Message>()
        assertThat(insert.offset.toString()).contains("cs:")
        val offset2 = MongoSourceOffset(insert.offset.toString(), insert.offset.dbColl)
        thread { ChangeStreamsReader(mongod.uri, "mydb.mycoll", offset2, m2).run() }
        // Resume update action
        Thread.sleep(1000)
        collection.deleteOne(Filters.eq("action", 2))
        Thread.sleep(1000)
        assertThat(m2).hasSize(2)

        val delete = m2.elementAt(1)
        assertThat(delete.oplog["ts"]).isInstanceOf(BsonTimestamp::class.java)
        assertThat(delete.oplog["ns"]).isEqualTo("mydb.mycoll")
        assertThat(delete.oplog["op"]).isEqualTo("d")
        assertThat((delete.oplog["o"] as Document)["_id"]).isInstanceOf(ObjectId::class.java)

        // Read with last timestamp
        val m3 = ConcurrentLinkedQueue<Message>()
        // Change to a non-exist resume token
        delete.offset.resumeToken!!["_data"] =
            BsonString("825E3CE25D000000012B022C0100296E5A100487FE456CA60A4DE8BE6B04D89910D45F46645F696400645E3CE25D026E0076836EC8610004")
        val offset3 = MongoSourceOffset(delete.offset.toString(), delete.offset.dbColl)
        thread { ChangeStreamsReader(mongod.uri, "mydb.mycoll", offset3, m3).run() }
        Thread.sleep(1000)
        collection.insertOne(Document("action", 2))
        Thread.sleep(1000)
        // Resume delete and insert action, changestreams started with timestamp will resume with ts: {$gte: Timestamp()}
        assertThat(m3).hasSize(2)
        val insert2 = m3.elementAt(1)
        assertThat((insert2.oplog["o"] as Document)["action"]).isEqualTo(2)
    }
}

