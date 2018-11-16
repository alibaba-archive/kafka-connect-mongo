package com.teambition.kafka.connect.mongo.source

import org.bson.BsonTimestamp
import org.bson.types.ObjectId
import java.lang.Integer.parseInt
import java.util.*

/**
 * Created by jentle on 9/27/17.
 */
class MongoSourceOffset(offsetStr: String?) {

    private val pieces = offsetStr?.split(SPLITOR.toRegex())

    private val timestamp: Int = pieces?.let { parseInt(it[0]) } ?: Math.floor(Date().time.toDouble() / 1000).toInt()
    private val inc: Int = pieces?.let { if (it.size > 1) parseInt(it[1]) else 0 } ?: 0
    val ts = BsonTimestamp(timestamp, inc)

    // To be compatible with old format
    val sortField: String = if (pieces != null && pieces.size > 2) pieces[2] else ""
    val finishedImport: Boolean = if (pieces != null && pieces.size > 3) parseInt(pieces[3]) > 0 else pieces != null

    override fun toString(): String {
        return toOffsetString(ts, sortField, finishedImport)
    }

    companion object {
        const val SPLITOR = ","

        /**
         * Start from current time will skip a lot of redundant scan on oplog
         * Format: LATEST_TIMESTAMP,INC,SORT_FIELD,FINISH_IMPORT
         */
        fun toOffsetString(ts: BsonTimestamp, sortField: String, finishedImport: Boolean): String {
            val finishedFlag = if (finishedImport) 1 else -1
            return "${ts.time}$SPLITOR${ts.inc}$SPLITOR$sortField$SPLITOR$finishedFlag"
        }
    }
}
