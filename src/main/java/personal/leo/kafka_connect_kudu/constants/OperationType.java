package personal.leo.kafka_connect_kudu.constants;

import java.util.Objects;

/**
 * copy from  io.debezium.data.Envelope.Operation
 */
public enum OperationType {
    /**
     * The operation that read the current state of a record, most typically during snapshots.
     */
    READ("r"),
    /**
     * An operation that resulted in a new record being created in the source.
     */
    CREATE("c"),
    /**
     * An operation that resulted in an existing record being updated in the source.
     */
    UPDATE("u"),
    /**
     * An operation that resulted in an existing record being removed from or deleted in the source.
     */
    DELETE("d");

    private final String value;

    OperationType(String value) {
        this.value = value;
    }

    public static OperationType of(String value) {
        for (OperationType operationType : values()) {
            if (Objects.equals(operationType.value, value)) {
                return operationType;
            }
        }
        throw new RuntimeException("not found:" + value);
    }
}
