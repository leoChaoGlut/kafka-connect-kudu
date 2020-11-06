package personal.leo.kafka_connect_kudu;

public interface PropDefaultValues {
    String maxBatchSize = "10000";
    String onlySyncValueChangedColumns = "false";
    String logEnabled = "false";
}
