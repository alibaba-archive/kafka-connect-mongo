package org.apache.kafka.connect.mongo;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.*;

/**
 * Created by Xu Jingxin on 16/8/17.
 */
public class DatabaseReaderTest {
    private DatabaseReader databaseReader;

    @Before
    public void setUp() throws Exception {
        databaseReader = new DatabaseReader("127.0.0.1",
                                            27017,
                                            "test",
                                            "0",
                                            new ConcurrentLinkedQueue<>());
    }

    @Test
    public void run() throws Exception {
        PowerMock.replayAll();
        databaseReader.run();
        PowerMock.verifyAll();
    }

}