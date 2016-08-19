package org.apache.kafka.connect.mongo;

import junit.framework.TestCase;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.powermock.api.easymock.PowerMock;

import java.util.*;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;

/**
 * Created by Xu Jingxin on 16/8/12.
 */
public class FileStreamSourceTaskTest extends TestCase {

    private static final String TOPIC = "topic_test";
    private static final String FILE = "./src/test/resources/text.txt";

    private FileStreamSourceConnector connector;
    private Map<String, String> sourceProperties;

    private OffsetStorageReader offsetStorageReader;

    private FileStreamSourceTask streamSource;

    @Override
    protected void setUp() {
        sourceProperties = new HashMap<>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC, TOPIC);
        sourceProperties.put(FileStreamSourceConnector.FILE, FILE);

        connector = new FileStreamSourceConnector();
        ConnectorContext connectorContext = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(connectorContext);

        streamSource = new FileStreamSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        SourceTaskContext sourceTaskContext = PowerMock.createMock(SourceTaskContext.class);

        streamSource.initialize(sourceTaskContext);
        streamSource.start(sourceProperties);

        expect(sourceTaskContext.offsetStorageReader()).andReturn(offsetStorageReader);
    }

    public void testSourceTasksWithNullOffset() {
        expectOffsetLookupReturnNone();
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertEquals(1, taskConfigs.size());
        assertEquals(FILE, taskConfigs.get(0).get(FileStreamSourceConnector.FILE));
        assertEquals(TOPIC, taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC));

        List<SourceRecord> records = new ArrayList<>();

        try {
            records = streamSource.poll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(2, records.size());

        for (Integer i = 0; i < records.size(); i++) {
            assertEquals(records.get(i).value(), "line " + (i + 1));
        }

        PowerMock.verifyAll();
    }

    public void testSourceTasksWithOffset() {
        expectOffsetLookupReturnOffset();
        PowerMock.replayAll();

        connector.start(sourceProperties);
        connector.taskConfigs(1);

        List<SourceRecord> records = new ArrayList<>();

        try {
            records = streamSource.poll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(1, records.size());
        assertEquals(records.get(0).value(), "line 2");

        PowerMock.verify();
    }

    private void expectOffsetLookupReturnNone() {
        expect(offsetStorageReader.offset(anyObject(HashMap.class))).andReturn(new HashMap<>());
    }

    private void expectOffsetLookupReturnOffset() {
        HashMap<String, Object> offset = new HashMap<>();
        offset.put(FileStreamSourceTask.POSITION_FIELD, 1L);
        expect(offsetStorageReader.offset(anyObject(HashMap.class))).andReturn(offset);
    }

}
