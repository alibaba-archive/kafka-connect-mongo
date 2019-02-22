package com.teambition.kafka.connect.mongo.source

import com.google.common.truth.Truth.assertThat
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.junit.Test

/**
 * @author Xu Jingxin
 */
class SchemaParserTest {

    @Test
    fun parse() {
        Schema(
            "mongo_slave_teambition_users-value",
            1,
            1,
            UserSchema.record
        )
            .let { SchemaParser.parse(it) }
            .let { schema ->
                assertThat(schema.name()).isEqualTo("mongo_schema_teambition_users")
                assertThat(schema.parameters()["table"]).isEqualTo("base_users")
                assertThat(schema.field("__op").schema().type().name).isEqualTo("STRING")
                assertThat(schema.field("__pkey").schema().type().name).isEqualTo("STRING")
                assertThat(schema.field("__pkey").schema().parameters()["sqlType"]).isEqualTo("VARCHAR(100)")
                assertThat(schema.field("name").schema().type().name).isEqualTo("STRING")
                assertThat(schema.field("name").schema().isOptional).isEqualTo(true)
                assertThat(schema.field("name").schema().parameters()["sqlType"]).isEqualTo("VARCHAR")
                assertThat(schema.field("updated").schema().parameters()["sqlType"]).isEqualTo("TIMESTAMP")
            }
    }
}
