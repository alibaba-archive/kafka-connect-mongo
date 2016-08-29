package org.apache.kafka.connect.mongo;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Xu Jingxin on 16/8/17.
 */
public class MongoSourceConnectorTest {
    private MongoSourceConnector connector;

    @Before
    public void setUp() throws Exception {
        connector = new MongoSourceConnector();
        ConnectorContext context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        Map<String, String> props = new HashMap<>();
        props.put("host", "localhost");
        props.put("port", "12345");
        props.put("batch.size", "100");
        props.put("schema.name", "schema");
        props.put("topic.prefix", "prefix");
        props.put("databases", "mydb.test1,mydb.test2,mydb.test3");

        connector.start(props);
    }

    @Test
    public void taskConfigs() throws Exception {
        PowerMock.replayAll();

        List<Map<String, String>> configs = connector.taskConfigs(2);

        assertEquals(2, configs.size());

        for (int i = 0; i < configs.size(); i++) {
            Map<String, String> config = configs.get(i);
            if (i == 0) {
                assertEquals("mydb.test1,mydb.test2", config.get(MongoSourceConfig.DATABASES_CONFIG));
            } else {
                assertEquals("mydb.test3", config.get(MongoSourceConfig.DATABASES_CONFIG));
            }
        }

        PowerMock.verifyAll();
    }

    @Test
    public void config() throws Exception {
        PowerMock.replayAll();

        ConfigDef config = connector.config();

        assertTrue(config.configKeys().keySet().contains("host"));
        assertTrue(config.configKeys().keySet().contains("port"));

        PowerMock.verifyAll();
    }

}