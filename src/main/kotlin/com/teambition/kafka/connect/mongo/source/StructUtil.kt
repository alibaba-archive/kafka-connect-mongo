package com.teambition.kafka.connect.mongo.source

import org.bson.Document

/**
 * @author Xu Jingxin
 */
object StructUtil {
    fun getDB(message: Document): String {
        return message["ns"] as String
    }

    fun getTopic(message: Document, topicPrefix: String): String {
        val db = getDB(message).replace(".", "_")
        return topicPrefix + "_" + db
    }
}
