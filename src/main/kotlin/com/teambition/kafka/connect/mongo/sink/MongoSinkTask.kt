package com.teambition.kafka.connect.mongo.sink

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import com.teambition.kafka.connect.mongo.database.MongoClientLoader
import com.teambition.kafka.connect.mongo.sink.MongoSinkConfig.Companion.DATABASES_CONFIG
import com.teambition.kafka.connect.mongo.sink.MongoSinkConfig.Companion.MONGO_URI_CONFIG
import com.teambition.kafka.connect.mongo.sink.MongoSinkConfig.Companion.SOURCE_TOPICS_CONFIG
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory

/**
 * @author Xu Jingxin
 */
class MongoSinkTask : SinkTask() {
    companion object {
        private val log = LoggerFactory.getLogger(MongoSinkTask::class.java)
    }

    private var uri = ""

    private var mongoClient: MongoClient? = null
    private var collections = mutableMapOf<String, MongoCollection<Document>>()
    private var topicMapToDb = mutableMapOf<String, String>()

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
                bulks[ns] = mutableListOf()
            }

            // Delete object by id if object is empty
            if (struct["object"] == null) {
                bulks[ns]!!.add(
                    DeleteOneModel<Document>(
                        Filters.eq("_id", ObjectId(id))
                    )
                )
                log.trace("Adding delete model to bulk: {}", id)
                continue
            }

            val flatObj = mutableMapOf<String, Any?>()
            try {
                (BasicDBObject.parse(struct["object"] as String) as Map<*, *>).mapKeysTo(flatObj) {
                    it.key.toString()
                }
            } catch (e: Exception) {
                log.error("JSON parse error: {}", struct["object"])
                continue
            }
            val doc = Document(flatObj)
            log.trace("Adding update model to bulk: {}", doc.toString())
            bulks[ns]!!.add(
                UpdateOneModel<Document>(
                    Filters.eq("_id", ObjectId(id)),
                    Document("\$set", doc),
                    UpdateOptions().upsert(true)
                )
            )
        }
        for ((ns, docs) in bulks) {
            try {
                val writeResult = getCollection(ns).bulkWrite(docs)
                log.trace("Write result: {}", writeResult)
            } catch (e: Exception) {
                log.error("Bulk write error {}", e.message)
            }
        }
    }

    override fun version(): String = MongoSinkConnector().version()

    override fun flush(offsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    override fun start(props: Map<String, String>) {
        log.trace("Parsing configuration: {}", props)
        uri = props.getValue(MONGO_URI_CONFIG)
        val topics = props.getValue(SOURCE_TOPICS_CONFIG).split(",")
        val databases = props.getValue(DATABASES_CONFIG).split(",")
        for ((i, topic) in topics.withIndex()) {
            topicMapToDb[topic] = databases[i]
        }
        mongoClient = MongoClientLoader.getClient(uri)
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
