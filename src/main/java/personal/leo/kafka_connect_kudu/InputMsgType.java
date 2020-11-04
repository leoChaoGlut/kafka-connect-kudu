package personal.leo.kafka_connect_kudu;

public enum InputMsgType {
    /**
     * 用于binlog同步
     */
    binlog,
    /**
     * json格式的数据集,如 {"col1":"value1","col2":"value2"}
     * 用于将流计算的结果写kudu
     */
    kafkaMsg
}
