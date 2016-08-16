package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/3.
 */
public class MongoSourceTask extends SourceTask{
    @Override
    public String version() {
        return new MongoSourceConnector().version();
    }

    /**
     * Parse the config properties into in-use type and format
     * @param props
     */
    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
