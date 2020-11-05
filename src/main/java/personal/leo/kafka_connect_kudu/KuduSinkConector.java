package personal.leo.kafka_connect_kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduSinkConector extends SinkConnector {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KuduSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    @Override
    public void stop() {
        logger.info("stop");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(PropKeys.masterAddresses, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kudu " + PropKeys.masterAddresses)
                .define(PropKeys.kuduTableName, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PropKeys.kuduTableName)
                .define(PropKeys.kafkaBrokers, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PropKeys.kafkaBrokers)
                .define(PropKeys.inputMsgType, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PropKeys.inputMsgType)
                ;
    }

    /**
     * TODO 后续考虑校验kudu table name 和topics.regex是否匹配
     *
     * @param connectorConfigs
     * @return
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        logger.info("validate: " + connectorConfigs);
        final Config config = super.validate(connectorConfigs);
        validateKudu(connectorConfigs);
        validateKafka(connectorConfigs);
        return config;
    }

    private void validateKafka(Map<String, String> connectorConfigs) {
        final String keyConverter = connectorConfigs.get(PropKeys.keyConverter);
        final String valueConverter = connectorConfigs.get(PropKeys.valueConverter);
        final InputMsgType inputMsgType = InputMsgType.valueOf(connectorConfigs.get(PropKeys.inputMsgType));
        final String legalKeyConverter, legalValueConverter;
        //TODO 需要确定key.converter和value.converter是否必填,如果非必填,可自行根据inputMsgType设置
        switch (inputMsgType) {
            case binlog:
                legalKeyConverter = JSONObjectConverter.class.getCanonicalName();
                legalValueConverter = JSONObjectConverter.class.getCanonicalName();
                if (!StringUtils.equals(legalKeyConverter, keyConverter)) {
                    throw new RuntimeException("keyConverter only support:" + legalKeyConverter + " for inputMsgType:" + inputMsgType);
                }
                if (!StringUtils.equals(legalValueConverter, valueConverter)) {
                    throw new RuntimeException("valueConverter only support:" + legalValueConverter + " for inputMsgType:" + inputMsgType);
                }
                break;
            case kafkaMsg:
                legalKeyConverter = StringConverter.class.getCanonicalName();
                legalValueConverter = StringConverter.class.getCanonicalName();
                if (!StringUtils.equals(legalKeyConverter, keyConverter)) {
                    throw new RuntimeException("keyConverter only support:" + legalKeyConverter + " for inputMsgType:" + inputMsgType);
                }
                if (!StringUtils.equals(legalValueConverter, valueConverter)) {
                    throw new RuntimeException("valueConverter only support:" + legalValueConverter + " for inputMsgType:" + inputMsgType);
                }
                break;
            default:
                throw new RuntimeException("not supported:" + inputMsgType);
        }


        final KafkaProducer kafkaProducer = new KafkaProducer(connectorConfigs);
        kafkaProducer.getProducer().flush();
        kafkaProducer.close();
    }

    private void validateKudu(Map<String, String> connectorConfigs) {
        final String masterAddresses = connectorConfigs.get(PropKeys.masterAddresses);
        final String kuduTableName = connectorConfigs.get(PropKeys.kuduTableName);
//        final boolean sendDataToKuduTableNameTopic = Boolean.parseBoolean(connectorConfigs.get(PropKeys.sendDataToKuduTableNameTopic));
        InputMsgType inputMsgType = InputMsgType.valueOf(connectorConfigs.get(PropKeys.inputMsgType));
        KuduClient kuduClient = null;
        try {
            kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();

            final boolean tableExists = kuduClient.tableExists(kuduTableName);
            if (!tableExists) {
                throw new RuntimeException("Kudu table not exists: " + kuduTableName);
            }
        } catch (Exception e) {
            logger.error("validate error", e);
            throw new RuntimeException(e);
        } finally {
            if (kuduClient != null) {
                try {
                    kuduClient.close();
                } catch (KuduException e) {
                    logger.error("validate error,in final block", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public String version() {
        //TODO 后续考虑在打包时自动获取project.version
        return "1";
    }

}
