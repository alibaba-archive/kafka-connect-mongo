package com.teambition.kafka.connect.mongo.utils

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.config.Storage
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import org.apache.commons.io.FileUtils
import java.io.File
import java.lang.Exception

/**
 * @author Xu Jingxin
 * Get client of embedded mongodb
 */
class Mongod {
    companion object {
        val collections = arrayOf("test1", "test2", "test3")
    }

    private val port = 12345
    val uri = "mongodb://localhost:$port"

    private val tmpFile = "tmp"
    private val starter = MongodStarter.getDefaultInstance()
    private val config = MongodConfigBuilder()
        .version(Version.Main.V3_6)
        .replication(Storage(tmpFile, "rs0", 1024))
        .net(Net(port, Network.localhostIsIPv6()))
        .build()
    private val executable = starter.prepare(config)
    private lateinit var process: MongodProcess
    private lateinit var client: MongoClient

    fun start(): Mongod {
        process = executable.start()
        client = MongoClient(MongoClientURI(uri))
        val admin = client.getDatabase("admin")
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
        Thread.sleep(3000)  // Wait for master election

        return this
    }

    fun stop() {
        try {
            client.close()
            process.stop()
            executable.stop()
        } catch (e: Exception) {
            // Ignore exception
        } finally {
            FileUtils.deleteDirectory(File(tmpFile))
        }
    }

    fun getDatabase(db: String): MongoDatabase {
        return client.getDatabase(db)
    }
}
