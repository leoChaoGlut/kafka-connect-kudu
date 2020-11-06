package personal.leo.kafka_connect_kudu;

public interface PropKeys {
    String masterAddresses = "masterAddresses";
    String kuduTableName = "kuduTableName";
    String maxBatchSize = "maxBatchSize";
    String onlySyncValueChangedColumns = "onlySyncValueChangedColumns";
    String logEnabled = "logEnabled";
    String kafkaBrokers = "kafkaBrokers";
    String sendDataToKuduTableNameTopic = "sendDataToKuduTableNameTopic";
    String inputMsgType = "inputMsgType";
    String locale = "locale";

    String keyConverter = "key.converter";
    String valueConverter = "value.converter";
}
