package org.apache.kafka.connect.mongo

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import com.mongodb.util.JSON
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.bson.Document
import org.slf4j.LoggerFactory
import org.apache.kafka.connect.mongo.MongoSinkConfig.Companion.MONGO_URI_CONFIG
import org.apache.kafka.connect.mongo.MongoSinkConfig.Companion.DATABASES_CONFIG
import org.apache.kafka.connect.mongo.MongoSinkConfig.Companion.SOURCE_TOPICS_CONFIG
import org.bson.types.ObjectId

interface MongoSinkTaskMBean {
    var mProps: String
    val mTopicMapToDb: MutableMap<String, String>
}

/**
 * @author Xu Jingxin
 */
class MongoSinkTask : SinkTask(), MongoSinkTaskMBean {
    companion object {
        private val log = LoggerFactory.getLogger(MongoSinkTask::class.java)
    }

    private var uri = ""

    private var mongoClient: MongoClient? = null
    private var collections = mutableMapOf<String, MongoCollection<Document>>()
    private var topicMapToDb = mutableMapOf<String, String>()

    override var mProps: String = ""
    override val mTopicMapToDb get() = topicMapToDb

    override fun put(records: Collection<SinkRecord>) {
        log.debug("Receive records {}", records.size)
        val bulks = mutableMapOf<String, MutableList<WriteModel<Document>>>()
        for (record in records) {
            log.trace("Put record: {}", record)
            val struct = record.value() as Struct
            val topic = record.topic()
            if (topicMapToDb[topic] == null) {
                throw Exception("Topic $topic is not defined in config.")
            }
            val ns = topicMapToDb[topic] as String
            val id = struct["id"] as String

            if (bulks[ns] == null) {
                bulks[ns] = mutableListOf<WriteModel<Document>>()
            }

            // Delete object by id if object is empty
            if (struct["object"] == null) {
                bulks[ns]!!.add(DeleteOneModel<Document>(
                        Filters.eq("_id", ObjectId(id))
                ))
                log.trace("Adding delete model to bulk: {}", id)
                continue
            }

            val flatObj = mutableMapOf<String, Any?>()
            try {
                (JSON.parse(struct["object"] as String) as Map<*, *>).mapKeysTo(flatObj) {
                    it.key.toString()
                }
            } catch (e: Exception) {
                log.error("JSON parse error: {}", struct["object"])
                continue
            }
            val doc = Document(flatObj)
            log.trace("Adding update model to bulk: {}", doc.toString())
            bulks[ns]!!.add(UpdateOneModel<Document>(
                    Filters.eq("_id", ObjectId(id)),
                    Document("\$set", doc),
                    UpdateOptions().upsert(true)
            ))
        }
        for ((ns, docs) in bulks) {
            try {
                val writeResult = getCollection(ns).bulkWrite(docs)
                log.trace("Write result: {}", writeResult)
            } catch (e: Exception) {
                // @todo Retry write messages
                log.error("Bulk write error {}", e.message)
            }
        }
    }

    override fun version(): String = MongoSinkConnector().version()

    override fun flush(offsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration: {}", props)
        mProps = props.toString()
        uri = props[MONGO_URI_CONFIG]!!
        val topics = props[SOURCE_TOPICS_CONFIG]!!.split(",")
        val databases = props[DATABASES_CONFIG]!!.split(",")
        for ((i, topic) in topics.withIndex()) {
            topicMapToDb[topic] = databases[i]
        }
        val clientOptions = MongoClientOptions.builder()
                .connectTimeout(1000 * 300)
        mongoClient = MongoClient(MongoClientURI(uri, clientOptions))
    }

    override fun stop() {
    }

    private fun getCollection(ns: String): MongoCollection<Document> {
        if (collections[ns] == null) {
            val (dbName, collectionName) = ns.split(".").dropLastWhile(String::isEmpty)
            collections[ns] = mongoClient!!.getDatabase(dbName).getCollection(collectionName)
        }
        return collections[ns]!!
    }
}