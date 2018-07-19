package com.teambition.kafka.connect.mongo.source

import com.google.common.truth.Truth.assertThat
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.Document
import org.bson.types.ObjectId
import org.junit.Test
import java.util.*

/**
 * @author Xu Jingxin
 */
class SchemaMapperTest {

    @Test
    fun aalyzeStruct() {
        val doc = Document(mapOf(
            "_id" to ObjectId("5b5005ceb9e80fb20d106896"),
            "string" to "string",
            "text" to "text",
            "int" to 10,
            "bool" to false,
            "double" to 1.1,
            "date" to Date(1531970947888), // 2018-07-19T03:29:07.888Z
            "array" to listOf("A", "B"),
            "vacuum" to null,
            "map" to mapOf("k" to "v"),
            "doc" to Document(mapOf("key" to "value")),
            "undefined" to BsonUndefined()
        ))
        val bson = Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.c",
            "op" to "i",
            "o" to doc
        ))
        // Test types mapping
        SchemaMapper
            .getAnalyzedStruct(bson, "schema_")
            .let {
                assertThat(it.schema().name()).isEqualTo("schema_d_c")
                assertThat(it.schema().parameters()["table"]).isEqualTo("base_d_c")
                assertThat(it["__op"]).isEqualTo("i")
                assertThat(it["__pkey"]).isEqualTo("5b5005ceb9e80fb20d106896")
                assertThat(it["__sql"]).isNull()
                assertThat(it["__ts"]).isEqualTo("2018-07-19T03:29:07.000Z")
                assertThat(it["_id"]).isEqualTo("5b5005ceb9e80fb20d106896")
                assertThat(it["string"]).isEqualTo("string")
                assertThat(it["text"]).isEqualTo("text")
                assertThat(it["int"]).isEqualTo(10.0)
                assertThat(it["bool"]).isEqualTo(false)
                assertThat(it["double"]).isEqualTo(1.1)
                assertThat(it["date"]).isEqualTo("2018-07-19T03:29:07.888Z")
                assertThat(it["array"]).isEqualTo("""["A","B"]""")
                assertThat(it.schema().fields()).doesNotContain("vacuum")
                assertThat(it["map"]).isEqualTo("""{"k":"v"}""")
                assertThat(it["doc"]).isEqualTo("""{ "key" : "value" }""")
                assertThat(it.schema().fields()).doesNotContain("undefined")
            }
    }

    @Test
    fun updateStruct() {
        // Add delete record
        Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.update",
            "op" to "d",
            "o" to Document(mapOf(
                "_id" to ObjectId("5b5005ceb9e80fb20d106896")
            ))
        )).let { SchemaMapper.getAnalyzedStruct(it, "schema_") }

        // Add insert record
        Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.update",
            "op" to "i",
            "o" to Document(mapOf(
                "_id" to ObjectId("5b5005ceb9e80fb20d106896"),
                "name" to "name"
            ))
        )).let { SchemaMapper.getAnalyzedStruct(it, "schema_") }
            .let {
                assertThat(it["name"]).isEqualTo("name")
            }
    }

    @Test
    fun conflictStruct() {
        // Schema type is double
        Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.conflict",
            "op" to "d",
            "o" to Document(mapOf(
                "name" to 10
            ))
        )).let { SchemaMapper.getAnalyzedStruct(it, "schema_") }
            .let { assertThat(it["name"]).isEqualTo(10.0) }

        // Schema type is boolean, will convert into string
        Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.conflict",
            "op" to "i",
            "o" to Document(mapOf(
                "name" to false
            ))
        )).let { SchemaMapper.getAnalyzedStruct(it, "schema_") }
            .let { assertThat(it["name"]).isEqualTo("false") }

        // New coming schema type of double will still use string
        Document(mapOf(
            "ts" to BsonTimestamp(1531970947, 1),
            "ns" to "d.conflict",
            "op" to "i",
            "o" to Document(mapOf(
                "name" to 20
            ))
        )).let { SchemaMapper.getAnalyzedStruct(it, "schema_") }
            .let { assertThat(it["name"]).isEqualTo("20.0") }
    }
}
