package personal.leo.kafka_connect_kudu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
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
    private boolean sendDataToKuduTableNameTopic;
    private InputMsgType inputMsgType;
    private EmailService emailService;
    private Map<String, String> props;
    private boolean includeSchemaChanges;
    private final List<String> msgs = new ArrayList<>();
    private final List<Operation> operations = new ArrayList<>(maxBatchSize);

    @Override
    public void start(Map<String, String> props) {
        try {
            reporter = context.errantRecordReporter(); // may be null if DLQ not enabled
        } catch (NoClassDefFoundError e) {
            logger.error("errantRecordReporter error", e);
            // Will occur in Connect runtimes earlier than 2.6
            reporter = null;
        }

        this.props = props;
        sendDataToKuduTableNameTopic = Boolean.parseBoolean(props.getOrDefault(PropKeys.sendDataToKuduTableNameTopic, PropDefaultValues.sendDataToKuduTableNameTopic));
        inputMsgType = InputMsgType.valueOf(props.get(PropKeys.inputMsgType));
        maxBatchSize = Integer.parseInt(props.getOrDefault(PropKeys.maxBatchSize, PropDefaultValues.maxBatchSize));
        includeSchemaChanges = Boolean.parseBoolean(props.getOrDefault(PropKeys.includeSchemaChanges, PropDefaultValues.includeSchemaChanges));

        emailService = new EmailService(
                props.getOrDefault(PropKeys.emailHostName, PropDefaultValues.emailHostName),
                props.getOrDefault(PropKeys.emailFrom, PropDefaultValues.emailFrom),
                props.getOrDefault(PropKeys.emailUser, PropDefaultValues.emailUser),
                props.getOrDefault(PropKeys.emailPassword, PropDefaultValues.emailPassword),
                StringUtils.splitByWholeSeparator(props.getOrDefault(PropKeys.emailTo, PropDefaultValues.emailTo), ",")
        );

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
        try {
            sync(records, msgs);
        } catch (Exception e) {
            final String msgJson = JSON.toJSONString(msgs);
            logger.error("put error, msgs:" + msgJson, e);
            emailService.send("props:" + props + "\n,error:" + e.getMessage() + "\n");
            throw new RuntimeException("Failed on record", e);
        }
    }

    private void sync(Collection<SinkRecord> records, List<String> msgs) throws KuduException {
        for (SinkRecord record : records) {
            if (record.value() == null) {
                continue;
            }
            final Operation operation;
            switch (inputMsgType) {
                case binlog:
                    final JSONObject value = (JSONObject) record.value();
                    if (value == null) {
                        continue;
                    }

                    final JSONObject payload = includeSchemaChanges ? value.getJSONObject(PayloadKeys.payload) : value;
                    if (payload == null) {
                        continue;
                    }

                    if (sendDataToKuduTableNameTopic) {
                        //只取insert,update的after部分,delete的不取
                        final JSONObject after = payload.getJSONObject(PayloadKeys.after);
                        if (after != null) {
                            kafkaProducer.send(after.toJSONString());
                        }
                    }

                    operation = kuduSyncer.createOperationByPayload(payload);
                    msgs.add(payload.toJSONString());
                    break;
                case kafkaMsg:
                    final String json = (String) record.value();
                    final Map dataSet = JSON.parseObject(json, Map.class);
                    operation = kuduSyncer.createOperationByDataSet(dataSet);
                    msgs.add(json);
                    break;
                default:
                    throw new RuntimeException("not supported: " + inputMsgType);
            }


            operations.add(operation);

            if (operations.size() >= maxBatchSize) {
                kuduSyncer.syncAndClear(operations);
                msgs.clear();
            }

        }

        if (operations.isEmpty()) {
            return;
        }

        kuduSyncer.syncAndClear(operations);
        msgs.clear();
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
