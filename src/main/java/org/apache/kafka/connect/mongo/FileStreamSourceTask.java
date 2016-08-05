package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/5.
 */
public class FileStreamSourceTask extends SourceTask {

    private String filename;
    private String topic;
    private InputStream stream;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE);
        topic = props.get(FileStreamSourceConnector.TOPIC);
        System.out.println("[Filename] " + filename);
        System.out.println("[Topic] " + topic);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
