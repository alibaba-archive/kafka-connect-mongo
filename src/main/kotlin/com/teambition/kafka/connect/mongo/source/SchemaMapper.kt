package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.Document
import org.bson.types.ObjectId
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Xu Jingxin
 */
object SchemaMapper {
    private val schemas = ConcurrentHashMap<String, SchemaBuilder>()
    private val log = LoggerFactory.getLogger(SchemaMapper::class.java)

    fun getAnalyzedStruct(message: Document, schemaPrefix: String): Struct {
        val ns = message["ns"] as String
        val schemaName = ns
            .replace(".", "_")
            .let { schemaPrefix + it }
        val oldSchema = getSchema(schemaName)
        val builder = SchemaBuilder
            .struct()
            .name(schemaName)
            .let { addMetaFields(it) }
            .parameter("table", getTable(ns))
            .let { analyze(it, message["o"] as Document) }
            .let { maybeUpdateSchema(oldSchema, it) }
        return Struct(builder.build()).let { fillinFields(it, message) }
    }

    /**
     * Add meta fields on schema
     */
    private fun addMetaFields(schema: SchemaBuilder): SchemaBuilder =
        schema
            .field("__op",
                SchemaBuilder.string().optional()
                    .parameter("sqlType", "VARCHAR(5)"))
            .field("__pkey",
                SchemaBuilder.string().optional()
                    .parameter("sqlType", "VARCHAR(100)"))
            .field("__sql",
                SchemaBuilder.string().optional()
                    .parameter("sqlType", "VARCHAR(999)"))
            .field("__ts",
                SchemaBuilder.string().optional()
                    .parameter("sqlType", "TIMESTAMP"))

    /**
     * Fill fields in the document
     */
    private fun fillinFields(struct: Struct, message: Document): Struct {
        val body = message["o"] as Document
        body["__op"] = message["op"]
        body["__pkey"] = body["_id"]
        body["__ts"] = ((message["ts"] as BsonTimestamp).time * 1000L).let { DateUtil.getISODate(it) }

        struct.schema().fields().forEach { field ->
            transformValue(body[field.name()], field.schema().type())
                ?.let { struct.put(field.name(), it) }
        }
        return struct
    }

    /**
     * Generate schema from body of document
     */
    private fun analyze(builder: SchemaBuilder, body: Document): SchemaBuilder {
        body.toSortedMap().forEach { key, value ->
            value
                .let { transformValue(it) }
                ?.let { buildSchema(it).parameter("sqlType", sqlType(it)) }
                ?.let { builder.field(key, it.build()) }
        }
        return builder
    }

    /**
     * Get sql table name from ns
     */
    private fun getTable(ns: String): String {
        val (db, table) = ns.split(".")
        return when (db) {
            "teambition" -> "base_$table"
            else -> "base_${db}_$table"
        }
    }

    /**
     * Transform value into sql supported type
     * The only returned type will be in set of string, date, number, boolean and null
     */
    private fun transformValue(value: Any?): Any? =
        when (value) {
            is ObjectId -> value.toString()
            is Date, is Boolean -> value
            is Number -> value.toDouble()
            is Document -> value.toJson()
            is Map<*, *> -> JSONObject(value).toString()
            is Collection<*> -> JSONArray(value).toString()
            is BsonUndefined -> null
            else -> value?.toString()
        }

    /**
     * Get transformed value by schema type
     */
    private fun transformValue(value: Any?, type: Type): Any? =
        try {
            transformValue(value).let { v ->
                when (type) {
                    Type.STRING -> when (v) {
                        is Date -> DateUtil.getISODate(v.time)
                        else -> v?.toString()
                    }
                    Type.FLOAT64 -> v as? Double
                    Type.BOOLEAN -> v as? Boolean
                    else -> v?.toString()
                }
            }
        } catch (e: Exception) {
            log.warn("Convert value `$value` to type `$type` error")
            null
        }

    /**
     * Interpret schema for registry from bson value
     * - string: string, date
     * - number: number
     * - bool: boolean
     * - null: null, undefined (should remove this key from data)
     */
    private fun buildSchema(value: Any): SchemaBuilder =
        when (value) {
            is Number -> SchemaBuilder.float64().optional()
            is Boolean -> SchemaBuilder.bool().optional()
            else -> SchemaBuilder.string().optional()
        }

    /**
     * Interpret column type for database (postgresql) from bson value
     * - varchar: string
     * - timestamp: date
     * - double: number
     * - boolean: boolean
     * - null: null, undefined (should remove this key from data)
     */
    private fun sqlType(value: Any): String =
        when (value) {
            is Date -> "TIMESTAMP"
            is Number -> "DOUBLE"
            is Boolean -> "BOOLEAN"
            else -> "VARCHAR"
        }

    /**
     * Check whether the old schema contains all the fields and types of the new schema
     * or merge two schemas into a new one
     * and save it in the schemas map
     */
    private fun maybeUpdateSchema(oldSchema: SchemaBuilder?, newSchema: SchemaBuilder): SchemaBuilder {
        if (oldSchema == null) return getSortedSchema(newSchema).let {
            setSchema(it)
        }
        var updated = false
        val conflictFields = mutableListOf<String>()

        newSchema.fields().forEach { field ->
            if (oldSchema.field(field.name()) == null) {
                // Meet new field
                oldSchema.field(field.name(), field.schema())
                updated = true
            } else if (oldSchema.field(field.name()) != null &&
                oldSchema.schema().field(field.name()).schema().type() != field.schema().type()) {
                // Schema conflict
                log.warn("Field `${field.name()}` of schema ${oldSchema.name()} is type conflicted")
                conflictFields.add(field.name())
                updated = true
            }
        }

        return when {
            conflictFields.isNotEmpty() -> getNonConflictSchema(oldSchema, conflictFields)
                .let { getSortedSchema(it) }
                .let { setSchema(it) }
            updated -> getSortedSchema(oldSchema)
                .let { setSchema(it) }
            else -> oldSchema
        }
    }

    /**
     * Get a new schema builder, which set all conflict fields to string
     */
    private fun getNonConflictSchema(schema: SchemaBuilder, conflictFields: List<String>): SchemaBuilder {
        val builder = SchemaBuilder.struct().name(schema.name())
        schema.fields().forEach {
            if (it.name() in conflictFields) {
                builder.field(it.name(), SchemaBuilder.string().optional())
            } else {
                builder.field(it.name(), it.schema())
            }
        }
        schema.parameters().forEach {
            builder.parameter(it.key, it.value)
        }
        return builder
    }

    /**
     * Resort schema by field name
     */
    private fun getSortedSchema(schema: SchemaBuilder): SchemaBuilder {
        val builder = SchemaBuilder.struct().name(schema.name())
        val fieldNames = schema.fields().map { it.name() }.sorted()
        fieldNames.forEach {
            builder.field(it, schema.field(it).schema())
        }
        schema.parameters().forEach {
            builder.parameter(it.key, it.value)
        }
        return builder
    }

    /**
     * Get saved schema from local map variable
     */
    private fun getSchema(name: String): SchemaBuilder? {
        return synchronized(schemas) {
            schemas[name]
        }
    }

    /**
     * Set new schema into local map variable
     */
    private fun setSchema(schema: SchemaBuilder): SchemaBuilder {
        return synchronized(schemas) {
            schemas[schema.name()] = schema
            schema
        }
    }
}

