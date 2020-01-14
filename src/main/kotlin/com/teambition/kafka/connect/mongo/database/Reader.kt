package com.teambition.kafka.connect.mongo.database

import com.teambition.kafka.connect.mongo.source.Message
import com.teambition.kafka.connect.mongo.source.MongoSourceOffset
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
abstract class Reader(
    val uri: String,
    val db: String,
    val start: MongoSourceOffset,
    val messages: ConcurrentLinkedQueue<Message>
)
