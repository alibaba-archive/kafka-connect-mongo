package com.teambition.kafka.connect.mongo.tools

import com.teambition.kafka.connect.mongo.database.MongoClientLoader
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.bson.BsonTimestamp
import org.bson.Document
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Export the whole collection into message queue
 * @author Xu Jingxin
 */
class CollectionExporter : Job {
    companion object {
        private val log = LoggerFactory.getLogger(CollectionExporter::class.java)
    }

    private lateinit var jobDataMap: CronJobDataMap
    private val maxSize = 3000

    override fun execute(context: JobExecutionContext) {
        log.info("Start export mongodb")
        run(context)
    }

    private fun run(context: JobExecutionContext) {
        jobDataMap = context.mergedJobDataMap["data"] as CronJobDataMap
        val mongoClient = MongoClientLoader.getClient(jobDataMap.uri)
        runBlocking {
            jobDataMap.databases.map { ns ->
                async(CommonPool) {
                    log.info("Export from database $ns")
                    val (db, collection) = ns.trim().split(".")
                    var count = 0
                    try {
                        mongoClient.getDatabase(db)
                            .getCollection(collection)
                            .find()
                            .forEach {
                                jobDataMap.messages.add(getPayload(ns, it))
                                count += 1
                                while (jobDataMap.messages.size > maxSize) delay(200)
                            }

                        log.info("Export database finish: {}, count {}", ns, count)
                    } catch (e: Exception) {
                        log.error("Export error: {}", e.toString())
                    }
                }
            }.forEach { it.await() }
        }
        log.info("Export finish")
    }

    private fun getPayload(ns: String, doc: Document): Document {
        return Document(mapOf(
            "ts" to BsonTimestamp(Math.floor((Date().time / 1000).toDouble()).toInt(), 0),
            "op" to "i",
            "ns" to ns,
            "o" to doc
        ))
    }
}
