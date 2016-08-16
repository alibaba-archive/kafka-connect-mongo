package org.apache.kafka.connect.mongo;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by Xu Jingxin on 16/8/5.
 */
public class FileStreamSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);

    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";

    private String filename;
    private String topic;
    private InputStream stream;
    private BufferedReader reader;
    private Long streamOffset;
    private int bulkSize = 1000;

    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE);
        topic = props.get(FileStreamSourceConnector.TOPIC);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            stream = new FileInputStream(filename);
            Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
            if (offset != null) {
                Object lastRecordedOffset = offset.get(POSITION_FIELD);
                if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) {
                    throw new ConnectException("Offset position is the incorrect type");
                }
                streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
            } else {
                streamOffset = 0L;
            }
            reader = new BufferedReader(new InputStreamReader(stream));
            log.debug("Opened {} for reading", filename);
        } catch (FileNotFoundException e) {
            log.warn("Could not find file for FileStreamSourceTask, sleeping");
            synchronized (this) {
                this.wait(1000);
            }
            return null;
        }

        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null) {
                return null;
            }
            log.trace("Read from offset {}", streamOffset);
            Long currentLine = 0L;
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (readerCopy.ready()) {
                String line;
                while ((line = readerCopy.readLine()) != null) {
                    currentLine += 1;
                    log.trace("Current line {}", currentLine);
                    if (currentLine <= streamOffset) continue;
                    log.trace("Read line {} {}", currentLine, line);
                    records.add(new SourceRecord(
                            Collections.singletonMap(FILENAME_FIELD, filename),
                            Collections.singletonMap(POSITION_FIELD, currentLine),
                            topic,
                            Schema.STRING_SCHEMA,
                            line
                    ));
                }
            }

            synchronized (this) {
                this.wait(1000);
            }
            return records;
        } catch (IOException e) {
            log.error("Read file content error {}", e);
        }

        return null;
    }

    @Override
    public void stop() {
        log.trace("Stopping now");
    }

}
