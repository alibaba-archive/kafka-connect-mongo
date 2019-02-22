package com.teambition.kafka.connect.mongo.utils

import com.mongodb.MongoException
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author Xu Jingxin
 */
object TaskUtil {
    private const val maxErrCount = 5
    // Reset error count if the task executed more than 5 minutes
    private const val minResetDuration = 600000L
    private val log = LoggerFactory.getLogger(TaskUtil::class.java)

    /**
     * Execute a job and rerun it when meet exception for several times
     */
    fun <R> runTry(name: String, errCount: Int = 0, block: () -> R): R {
        if (errCount > maxErrCount) throw Exception("Task [$name] raised too much errors!")
        val start = Date().time
        return try {
            block()
        } catch (e: MongoException) {
            log.error("Task execution error for {} times: {}", errCount + 1, e.toString())
            e.printStackTrace()
            if (Date().time - start > minResetDuration) {
                runTry(name, 1, block)
            } else {
                runTry(name, errCount + 1, block)
            }
        }
    }
}
