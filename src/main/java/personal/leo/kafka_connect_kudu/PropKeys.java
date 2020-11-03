package personal.leo.kafka_connect_kudu;

public interface PropKeys {
    String masterAddresses = "masterAddresses";
    String kuduTableName = "kuduTableName";
    String maxBatchSize = "maxBatchSize";
    String onlySyncValueChangedColumns = "onlySyncValueChangedColumns";
    String logEnabled = "logEnabled";
    String kafkaBrokers = "kafkaBrokers";
}
