package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.connect.data.Schema
import java.util.concurrent.ConcurrentHashMap

/**
 * @author Xu Jingxin
 */
object CachedSchema {

    private val schemas = ConcurrentHashMap<String, Schema>()
    /**
     * Get saved schema from local map variable
     */
    fun get(name: String): Schema? = synchronized(schemas) {
        schemas[name]
    }

    /**
     * Set new schema into local map variable
     */
    fun set(schema: Schema): Schema = synchronized(schemas) {
        schemas[schema.name()] = schema
        schema
    }
}
