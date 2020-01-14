package com.teambition.kafka.connect.mongo.source

import org.bson.Document

/**
 * @author Xu Jingxin
 */
data class Message(
    val offset: MongoSourceOffset,
    val oplog: Document
)
