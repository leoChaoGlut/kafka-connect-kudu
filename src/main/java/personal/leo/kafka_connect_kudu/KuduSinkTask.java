package personal.leo.kafka_connect_kudu;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KuduSinkTask extends SinkTask {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private KuduSyncer kuduSyncer;
    private ErrantRecordReporter reporter;
    private int maxBatchSize;
    private KafkaProducer kafkaProducer;


    @Override
    public void start(Map<String, String> props) {
        try {
            reporter = context.errantRecordReporter(); // may be null if DLQ not enabled
        } catch (NoClassDefFoundError e) {
            logger.error("errantRecordReporter error", e);
            // Will occur in Connect runtimes earlier than 2.6
            reporter = null;
        }

        maxBatchSize = Integer.parseInt(props.getOrDefault(PropKeys.maxBatchSize, PropDefaultValues.maxBatchSize));

        kafkaProducer = new KafkaProducer(props);

        try {
            kuduSyncer = new KuduSyncer(props);
        } catch (KuduException e) {
            logger.error("new KuduSyncer error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        final List<Operation> operations = new ArrayList<>(maxBatchSize);
        for (SinkRecord record : records) {
            try {
                if (record.value() == null) {
                    continue;
                }
                final JSONObject value = (JSONObject) record.value();
                if (value == null) {
                    continue;
                }
                final JSONObject payload = value.getJSONObject(PayloadKeys.payload);
                if (payload == null) {
                    continue;
                }

                kafkaProducer.send(payload.toJSONString());

                final Operation operation = kuduSyncer.createOperation(payload);
                operations.add(operation);

                if (operations.size() >= maxBatchSize) {
                    kuduSyncer.sync(operations);
                    operations.clear();
                }
            } catch (Exception e) {
                //TODO 邮件告警
                logger.error("put error", e);
                if (reporter != null) {
                    // Send errant record to error reporter
                    reporter.report(record, e);
                } else {
                    // There's no error reporter, so fail
                    throw new RuntimeException("Failed on record", e);
                }
            }
        }

        if (operations.isEmpty()) {
            return;
        }

        try {
            kuduSyncer.sync(operations);
            operations.clear();
        } catch (KuduException e) {
            logger.error("final sync error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (kuduSyncer == null) {
            return;
        }

        try {
            kuduSyncer.stop();
        } catch (KuduException e) {
            logger.error("stop error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String version() {
        //TODO 后续考虑在打包时自动获取project.version
        return "1";
    }
}
