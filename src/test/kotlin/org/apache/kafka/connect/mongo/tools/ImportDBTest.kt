package org.apache.kafka.connect.mongo.tools

import org.junit.Test

import org.junit.Assert.*

/**
 * @author Xu Jingxin
 */
class ImportDBTest {
    @Test
    fun run() {
        return
        val importJob = ImportDB("mongodb://localhost:27017", "test.users")
        importJob.run()
    }
}