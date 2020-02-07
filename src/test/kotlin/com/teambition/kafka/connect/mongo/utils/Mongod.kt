package com.teambition.kafka.connect.mongo.utils

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.MongoCommandException

/**
 * @author Xu Jingxin
 * Get client of embedded mongodb
 */
class Mongod {
    private val port = 12345
    val uri = "mongodb://127.0.0.1:$port"

    fun initialize(): Mongod {
        val client = MongoClient(MongoClientURI(uri))
        val admin = client.getDatabase("admin")
        try {
            val statusCMD = mapOf<Any, Any>(
                "replSetGetStatus" to 1,
                "initialSync" to 1
            )
            admin.runCommand(BasicDBObject(statusCMD))
        } catch (e: MongoCommandException) {
            // Has not initalized
            val config = mapOf<Any, Any>(
                "replSetInitiate" to mapOf(
                    "_id" to "rs0",
                    "members" to listOf(
                        mapOf(
                            "_id" to 0,
                            "host" to "127.0.0.1:$port"
                        )
                    )
                )
            )
            admin.runCommand(BasicDBObject(config))
            Thread.sleep(1000)  // Wait for master election
        } finally {
            client.close()
            return this
        }
    }
}
