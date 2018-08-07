package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject

/**
 * @author Xu Jingxin
 */
object SchemaParser {

    @Throws(JSONException::class)
    fun parse(schema: io.confluent.kafka.schemaregistry.client.rest.entities.Schema): Schema {
        val struct = JSONObject(schema.schema)
        val builder = SchemaBuilder.struct()
            .name(struct["name"] as String)
        // Set parameters
        setParameters(builder, struct)
        // Set fields
        val fields = struct["fields"] as JSONArray
        fields.map { it as JSONObject }
            .forEach { field ->
                val type = field["type"]
                when (type) {
                    is JSONArray -> parseType(type)
                    is JSONObject -> parseType(type)
                    is String -> parseType(type)
                    else -> null
                }?.also {
                    if (!field.isNull("default")) {
                        it.defaultValue(field["default"])
                    }
                }?.let {
                    val name = field["name"] as String
                    builder.field(name, it)
                }
            }
        return builder.build()
    }

    /**
     * Parse from string to schema type
     * all types are defined in SchemaMapper.kt
     */
    private fun parseType(type: String): SchemaBuilder =
        when (type) {
            "double" -> SchemaBuilder.float64()
            "boolean" -> SchemaBuilder.bool()
            else -> SchemaBuilder.string()
        }

    private fun parseType(structs: JSONArray): SchemaBuilder {
        val mainStruct = structs.find {
            when (it) {
                "null" -> false
                is JSONObject -> true
                is String -> true
                else -> false
            }
        }
        val builder = when (mainStruct) {
            is String -> parseType(mainStruct)
            is JSONObject -> parseType(mainStruct)
            else -> SchemaBuilder.string()
        }

        if (structs.contains("null")) {
            builder.optional()
        }

        return builder
    }

    // From JSONObject to typed field
    private fun parseType(struct: JSONObject): SchemaBuilder {
        val type = struct["type"] as String
        val builder = parseType(type)
        setParameters(builder, struct)
        return builder
    }

    private fun setParameters(builder: SchemaBuilder, struct: JSONObject): SchemaBuilder {
        if (struct.has("connect.parameters")) {
            val parameters = struct["connect.parameters"] as JSONObject
            parameters.toMap().forEach { key, value ->
                builder.parameter(key as String, value as String)
            }
        }
        return builder
    }
}
