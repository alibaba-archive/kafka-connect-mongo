package com.teambition.kafka.connect.mongo.source

import org.bson.Document

/**
 * @author Xu Jingxin
 */
object StructUtil {
    fun getDB(oplog: Document): String {
        return oplog["ns"] as String
    }

    fun getTopic(oplog: Document, topicPrefix: String): String {
        val db = getDB(oplog).replace(".", "_")
        return topicPrefix + "_" + db
    }
}
