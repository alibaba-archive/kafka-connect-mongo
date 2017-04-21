package org.apache.kafka.connect.mongo.utils

import com.mongodb.*
import com.mongodb.client.MongoDatabase
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.IMongodConfig
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.config.Storage
import de.flapdoodle.embed.process.runtime.Network
import org.apache.commons.io.FileUtils
import java.io.File

/**
 * @author Xu Jingxin
 */

class Mongod {

    companion object {
        val collections = arrayOf("test1", "test2", "test3")
    }

    private val REPLICATION_PATH = "tmp"

    private var mongodExecutable: MongodExecutable? = null
    private var mongodProcess: MongodProcess? = null
    private var mongodStarter: MongodStarter? = null
    private var mongodConfig: IMongodConfig? = null
    private var mongoClient: MongoClient? = null

    fun start() : Mongod {
        mongodStarter = MongodStarter.getDefaultInstance()
        mongodConfig = MongodConfigBuilder()
                .version(Version.Main.V3_3)
                .replication(Storage(REPLICATION_PATH, "rs0", 1024))
                .net(Net(12345, Network.localhostIsIPv6()))
                .build()
        mongodExecutable = mongodStarter!!.prepare(mongodConfig)
        mongodProcess = mongodExecutable!!.start()
        mongoClient = MongoClient(ServerAddress("localhost", 12345))

        // Initialize rs0
        val adminDatabase = mongoClient!!.getDatabase("admin")
        val replicaSetSetting = BasicDBObject()
        val members = BasicDBList()
        val host = BasicDBObject()
        replicaSetSetting.put("_id", "rs0")
        host.put("_id", 0)
        host.put("host", "127.0.0.1:12345")
        members.add(host)
        replicaSetSetting.put("members", members)
        adminDatabase.runCommand(BasicDBObject("isMaster", 1))
        adminDatabase.runCommand(BasicDBObject("replSetInitiate", replicaSetSetting))

        return this
    }

    fun stop() : Mongod {
        mongodProcess!!.stop()
        mongodExecutable!!.stop()
        FileUtils.deleteDirectory(File(REPLICATION_PATH))
        return this
    }

    fun createUserWithPassword() : Mongod {
        val adminDatabase = mongoClient!!.getDatabase("admin")
        val cmdArguments = BasicDBObject()
        cmdArguments.put("createUser", "test")
        cmdArguments.put("pwd", "123456")
        cmdArguments.put("roles", listOf("readWrite"))
        adminDatabase.runCommand(cmdArguments)
        return this
    }

    fun getDatabase(db: String): MongoDatabase {
        return mongoClient!!.getDatabase(db)
    }
}