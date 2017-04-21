package org.apache.kafka.connect.mongo.interfaces

import org.bson.Document
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * @author Xu Jingxin
 */
abstract class DatabaseRunner(uri: String,
                              db: String,
                              messages: ConcurrentLinkedQueue<Document>): Runnable {
    abstract fun stop()
}