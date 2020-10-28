package personal.leo.kafka_connect_kudu;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
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
                .define(PropKeys.kuduTableName, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PropKeys.masterAddresses)
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
        final Config config = super.validate(connectorConfigs);
        final String masterAddresses = connectorConfigs.get(PropKeys.masterAddresses);
        final String kuduTableName = connectorConfigs.get(PropKeys.kuduTableName);
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

        return config;
    }

    @Override
    public String version() {
        //TODO 后续考虑在打包时自动获取project.version
        return "1";
    }

}
