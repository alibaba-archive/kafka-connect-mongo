package com.teambition.kafka.connect.mongo.source

import org.apache.kafka.connect.connector.ConnectorContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.powermock.api.easymock.PowerMock
import java.util.*

/**
 * @author Xu Jingxin
 */
class MongoSourceConnectorTest {
    private var connector: MongoSourceConnector? = null

    @Before
    fun setUp() {
        connector = MongoSourceConnector()
        val context = PowerMock.createMock(ConnectorContext::class.java)
        connector!!.initialize(context)

        val props = HashMap<String, String>()
        props["mongo.uri"] = "mongodb://localhost:12345"
        props["initial.import"] = "true"
        props["batch.size"] = "100"
        props["schema.name"] = "schema"
        props["topic.prefix"] = "prefix"
        props["databases"] = "mydb.test1,mydb.test2,mydb.test3"

        connector!!.start(props)
    }

    @Test
    fun taskConfigs() {
        PowerMock.replayAll()

        val configs = connector!!.taskConfigs(2)

        assertEquals(2, configs.size.toLong())

        for (i in configs.indices) {
            val config = configs[i]
            if (i == 0) {
                assertEquals("mydb.test1,mydb.test2", config[MongoSourceConfig.DATABASES_CONFIG])
            } else {
                assertEquals("mydb.test3", config[MongoSourceConfig.DATABASES_CONFIG])
            }
        }

        PowerMock.verifyAll()
    }

    @Test
    fun config() {
        PowerMock.replayAll()

        val config = connector!!.config()

        assertTrue(config.configKeys().keys.contains("mongo.uri"))

        PowerMock.verifyAll()
    }

}
