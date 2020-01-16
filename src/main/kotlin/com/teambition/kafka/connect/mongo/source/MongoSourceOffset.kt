package com.teambition.kafka.connect.mongo.source

import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Integer.parseInt
import java.util.*

/**
 * Created by jentle on 9/27/17.
 * If start in the old format 'latest_timestamp,inc', use oplog tailing by default
 * If start in the new format 'latest_timestamp,inc,object_id,finished_import':
 *    if finished_import is true, use oplog tailing and update latest_timestamp
 *    else start mongo collection import from the object_id first then tailing
 */
class MongoSourceOffset() {
    companion object {
        val log: Logger = LoggerFactory.getLogger(MongoSourceOffset::class.java)
    }

    /**
     * Offset Pattern
     * 1578990230,1 (Oldest offset only contains timestamp and inc)
     * 1578990230,1,5e1d7a96e619dad727219d36,0 (New offset contains timestamp,inc,offsetid,flag of finish initial import)
     * 1578990230,1,cs:{"_data":{...}} (New offset read from change streams, start with cs, contains resume id of change streams)
     */
    constructor(offsetString: String, dbColl: String) : this() {
        this.dbColl = dbColl
        val pieces = offsetString.trim().split(",".toRegex())

        val timestamp: Int = parseInt(pieces[0])
        val inc: Int = pieces.let { if (it.size > 1) parseInt(it[1]) else 0 }
        ts = BsonTimestamp(timestamp, inc)

        if (pieces.size > 2 && pieces[2].startsWith("cs:")) {
            // Change streams
            this.resumeToken = offsetString.split("cs:").last().let {
                try {
                    BsonDocument.parse(it)
                } catch (e: Exception) {
                    log.error("Parse resume token error: {}, offset: {}", e.message, it)
                    null
                }
            }
            finishedImport = true
        } else {
            // Initial import
            objectId = if (pieces.size > 2) ObjectId(pieces[2]) else objectId
            finishedImport = if (pieces.size > 3) parseInt(pieces[3]) > 0 else finishedImport
        }
    }

    /**
     * Must wait for finished initial import then read from oplog
     * @param oplog Document with the format of oplog
     */
    constructor(oplog: Document, finishedImport: Boolean) : this() {
        ts = oplog["ts"] as BsonTimestamp
        dbColl = oplog["ns"] as String
        this.finishedImport = finishedImport
    }

    var dbColl = ""
    var ts = BsonTimestamp((Date().time / 1000).toInt(), 0)
    var objectId = ObjectId("000000000000000000000000")
    var resumeToken: BsonDocument? = null
    var finishedImport = false

    /**
     * Start from current time will skip a lot of redundant scan on oplog
     * Format: LATEST_TIMESTAMP,INC,OBJECT_ID,FINISH_IMPORT
     */
    override fun toString(): String =
        resumeToken?.let {
            "${ts.time},${ts.inc},cs:${it.toJson()}"
        } ?: let {
            val finishedFlag = if (finishedImport) 1 else 0
            "${ts.time},${ts.inc},$objectId,$finishedFlag"
        }

    // To real offset used in kafka
    fun toOffset(): Map<String, String> {
        return Collections.singletonMap(dbColl, this.toString())
    }
}
