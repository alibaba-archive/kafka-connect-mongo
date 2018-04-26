package com.teambition.kafka.connect.mongo

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Xu Jingxin
 */
object MongoClientLoader {
    private val defaultOptions = MongoClientOptions.builder()
        .connectTimeout(1000 * 300)
    private val clients = ConcurrentHashMap<String, MongoClient>()

    fun getClient(uri: String, options: MongoClientOptions.Builder = defaultOptions, reconnect: Boolean = false) =
        synchronized(clients) {
            if (reconnect) {
                clients[uri] = MongoClient(MongoClientURI(uri, options))
                clients[uri]
            } else {
                clients.getOrPut(uri) {
                    MongoClient(MongoClientURI(uri, options))
                }
            }
        }!!
}
