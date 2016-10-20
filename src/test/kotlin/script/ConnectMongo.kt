package script

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.ServerAddress

/**
 * @author Xu Jingxin
 */

fun connectWithPassword() {
    println("Connect mongo with password")
    val db = MongoClient(
            MongoClientURI("mongodb://root:root@192.168.0.21:27017/?replicaSet=rs0")
    )
    val user = db.getDatabase("test").getCollection("users")
    println(user.find().first())
}

fun connectWithCert() {
    println("Connect mongo with cert")
    System.setProperty("javax.net.ssl.trustStore", "")
}

fun main(args: Array<String>) {
    connectWithPassword()
}