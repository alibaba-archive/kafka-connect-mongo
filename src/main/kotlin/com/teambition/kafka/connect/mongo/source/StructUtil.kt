package com.teambition.kafka.connect.mongo.source

import org.bson.Document

/**
 * @author Xu Jingxin
 */
object StructUtil {
    private val ejsonAsMillis = Regex("\\{\\s*\"\\\$date\"\\s*:\\s*(\\d+)\\s*}")

    fun getDB(message: Document): String {
        return message["ns"] as String
    }

    fun getTopic(message: Document, topicPrefix: String): String {
        val db = getDB(message).replace(".", "_")
        return topicPrefix + "_" + db
    }

    fun dateTimeAsMillis(json: String): String {
      return ejsonAsMillis.replace(json, "\$1")
    }
}
