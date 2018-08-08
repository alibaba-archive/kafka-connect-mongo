package com.teambition.kafka.connect.mongo.source

import java.text.SimpleDateFormat
import java.util.*

/**
 * @author Xu Jingxin
 */
object DateUtil {
    private val isoDF: SimpleDateFormat
        get() {
            val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            df.timeZone = TimeZone.getTimeZone("UTC")
            return df
        }

    fun getISODate(ts: Long): String = isoDF.format(ts)

    fun format(text: String): String = getISODate(parse(text).time)

    fun parse(text: String): Date = isoDF.parse(text)
}

