package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.connector.ConnectorContext
import org.junit.Test

import org.junit.Assert.*
import org.powermock.api.easymock.PowerMock

/**
 * @author Xu Jingxin
 */
class MongoSinkConnectorTest {
    @Test
    fun taskConfigs() {
        val connector = MongoSinkConnector()
        val context = PowerMock.createMock(ConnectorContext::class.java)
        connector.initialize(context)

        val props = mutableMapOf<String, String>()
        props.put("mongo.uri", "mongodb://localhost:12345")
        props.put("topics", "a,b,c")
        props.put("databases", "t.a,t.b,t.c")
        connector.start(props)

        PowerMock.replayAll()
        val configs = connector.taskConfigs(2)
        assertEquals(2, configs.size)
        assertEquals("a,b", configs[0][MongoSinkConfig.SOURCE_TOPICS_CONFIG])
        assertEquals("c", configs[1][MongoSinkConfig.SOURCE_TOPICS_CONFIG])
        configs.forEach {
            assertEquals("mongodb://localhost:12345", it[MongoSinkConfig.MONGO_URI_CONFIG])
        }

        PowerMock.verifyAll()
    }
}