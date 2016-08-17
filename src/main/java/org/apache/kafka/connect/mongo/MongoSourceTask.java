package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/3.
 */
public class MongoSourceTask extends SourceTask{
    private final Logger log = LoggerFactory.getLogger(MongoSourceTask.class);

    private Integer port;

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
        log.trace("Parsing configuration");

        try {
            port = Integer.parseInt(props.get(MongoSourceConnector.PORT_CONFIG));
        } catch (Exception e) {
            throw new ConnectException(MongoSourceConnector.PORT_CONFIG + " config should be an Integer");
        }

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
