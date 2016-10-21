package script

import com.mongodb.*

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
    System.setProperty("javax.net.ssl.trustStore", "/Users/tristan/coding/teambition/kafka-connect-mongo/src/test/resources/truststore.jks")
    System.setProperty("javax.net.ssl.trustStorePassword", "123456")
    System.setProperty("javax.net.ssl.keyStore", "/Users/tristan/coding/teambition/kafka-connect-mongo/src/test/resources/keystore.ks")
    System.setProperty("javax.net.ssl.keyStorePassword", "123456")
    val client = MongoClient(MongoClientURI("mongodb://root:root@192.168.0.21:27017/?ssl=true&authSource=admin&replicaSet=rs0&sslInvalidHostNameAllowed=true"))
    val user = client.getDatabase("test").getCollection("users")
    while (true) {
        println(user.find().first())
        Thread.sleep(1000)
    }
}

fun main(args: Array<String>) {
    connectWithPassword()
    connectWithCert()
}