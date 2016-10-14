package org.apache.kafka.connect.mongo

import org.apache.kafka.connect.connector.ConnectorContext
import org.junit.Before
import org.junit.Test
import org.powermock.api.easymock.PowerMock

import java.util.HashMap

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue

/**
 * Created by Xu Jingxin on 16/8/17.
 */
class MongoSourceConnectorTest {
    private var connector: MongoSourceConnector? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        connector = MongoSourceConnector()
        val context = PowerMock.createMock(ConnectorContext::class.java)
        connector!!.initialize(context)

        val props = HashMap<String, String>()
        props.put("host", "localhost")
        props.put("port", "12345")
        props.put("batch.size", "100")
        props.put("schema.name", "schema")
        props.put("topic.prefix", "prefix")
        props.put("databases", "mydb.test1,mydb.test2,mydb.test3")

        connector!!.start(props)
    }

    @Test
    @Throws(Exception::class)
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
    @Throws(Exception::class)
    fun config() {
        PowerMock.replayAll()

        val config = connector!!.config()

        assertTrue(config.configKeys().keys.contains("host"))
        assertTrue(config.configKeys().keys.contains("port"))

        PowerMock.verifyAll()
    }

}