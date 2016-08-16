package org.apache.kafka.connect.mongo;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/5.
 */
public class FileStreamSourceConnector extends SourceConnector {

    public static final String FILE = "file";
    public static final String TOPIC = "topic";

    private String filename;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE);
        topic = props.get(TOPIC);
        if (filename == null || filename.isEmpty()) {
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        }
        if (topic == null || topic.isEmpty()) {
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        }
        if (topic.contains(",")) {
            throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        if (filename != null) {
            config.put(FILE, filename);
        }
        config.put(TOPIC, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }
}
