package org.apache.kafka.connect.mongo

import org.bson.Document
import org.junit.Before
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Created by Xu Jingxin on 16/8/17.
 */
class DatabaseReaderTest {
    private var databaseReader: DatabaseReader? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        databaseReader = DatabaseReader("127.0.0.1",
                27017,
                "test",
                "0.0",
                ConcurrentLinkedQueue<Document>())
    }

}