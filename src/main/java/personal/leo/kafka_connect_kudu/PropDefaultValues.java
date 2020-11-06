package personal.leo.kafka_connect_kudu;

import java.util.Locale;

public interface PropDefaultValues {
    String maxBatchSize = "10000";
    String onlySyncValueChangedColumns = "false";
    String logEnabled = "true";
    String locale = Locale.CHINA.toLanguageTag();
}
