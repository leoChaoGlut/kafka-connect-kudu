package personal.leo.kafka_connect_kudu;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KuduSyncer {
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String[] datePatterns = {
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
            DEFAULT_DATE_PATTERN,
    };
    private final KuduClient kuduClient;
    private final KuduSession session;
    private final KuduTable kuduTable;

    private final String masterAddresses;
    /**
     * TODO 需要非常明确topic和kuduTable的关系,否则可能出现数据错乱,需要拿kudu表名与topics.regex进行校验
     */
    private final String kuduTableName;
    private final int maxBatchSize;
    //TODO 如果kudu插入有性能问题,可以考虑在update的时候,减轻服务端压力,做columnDiff,仅把更新的字段做upsert
    private final boolean onlySyncValueChangedColumns;
    private final boolean logEnabled;
    private final Map<String, ColumnSchema> kuduColumnNameMapKuduColumn;
    private final SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_PATTERN);


    public KuduSyncer(Map<String, String> props) throws KuduException {
        masterAddresses = props.get(PropKeys.masterAddresses);
        kuduTableName = props.get(PropKeys.kuduTableName);
        maxBatchSize = Integer.parseInt(props.getOrDefault(PropKeys.maxBatchSize, PropDefaultValues.maxBatchSize)) + 10;//随便加几个size,防止kudu 报 超出maxBatchSize的错误
        onlySyncValueChangedColumns = Boolean.parseBoolean(props.getOrDefault(PropKeys.onlySyncValueChangedColumns, PropDefaultValues.onlySyncValueChangedColumns));
        logEnabled = Boolean.parseBoolean(props.getOrDefault(PropKeys.logEnabled, PropDefaultValues.logEnabled));

        final String zoneId = props.getOrDefault(PropKeys.zoneId, PropDefaultValues.zoneId);
        sdf.setTimeZone(TimeZone.getTimeZone(zoneId));

        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();

        kuduTable = kuduClient.openTable(kuduTableName);

        session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(maxBatchSize);

        kuduColumnNameMapKuduColumn = kuduTable.getSchema().getColumns().stream().collect(Collectors.toMap(columnSchema -> columnSchema.getName().toLowerCase(), Function.identity()));
        logger.info("KuduSyncer : " + toString());
    }


    public Operation createOperationByPayload(JSONObject payload) {
        final Map<String, Object> beforeColumnNameMapColumnValue = payload.getObject(PayloadKeys.before, Map.class);
        final Map<String, Object> afterColumnNameMapColumnValue = payload.getObject(PayloadKeys.after, Map.class);

        final Operation operation;
        final String op = payload.getString(PayloadKeys.op);
        final OperationType operationType = OperationType.of(op);

        final Map<String, Object> columnNameMapColumnValue;
        switch (operationType) {
            case CREATE:
            case UPDATE:
                columnNameMapColumnValue = afterColumnNameMapColumnValue;
                operation = kuduTable.newUpsert();
                break;
            case DELETE:
                columnNameMapColumnValue = beforeColumnNameMapColumnValue;
                operation = kuduTable.newDelete();
                break;
            default:
                throw new RuntimeException("not supported:" + operationType);
        }

        boolean hasAddData = false;
        for (Map.Entry<String, Object> entry : columnNameMapColumnValue.entrySet()) {
            final String srcColumnName = entry.getKey().toLowerCase();
            final Object srcColumnValue = entry.getValue();
            final ColumnSchema kuduColumn = kuduColumnNameMapKuduColumn.get(srcColumnName);
            if (kuduColumn == null) {
//                throw new RuntimeException("no column found for : " + srcColumnName);
//                TODO 发现不存在的列,可能源库出现变更,需要发邮件通知
            } else {
                fillRow(kuduColumn, srcColumnValue, operation.getRow());
//                row.addObject(kuduColumn.getName(), srcColumnValue);
                if (!hasAddData) {
                    hasAddData = true;
                }
            }
        }

        if (hasAddData) {
            return operation;
        } else {
            throw new RuntimeException("no column value be set,please confirm the topics are match the kudu table: " + kuduTableName);
        }
    }


    public Operation createOperationByDataSet(final Map<String, Object> dataSet) {
        final Operation operation = kuduTable.newUpsert();
        boolean hasAddData = false;
        for (Map.Entry<String, Object> entry : dataSet.entrySet()) {
            final String columnName = entry.getKey().toLowerCase();
            final Object columnValue = entry.getValue();
            final ColumnSchema kuduColumn = kuduColumnNameMapKuduColumn.get(columnName);
            if (kuduColumn == null) {
//                throw new RuntimeException("no column found for : " + columnName);
//                TODO 发现不存在的列,可能源库出现变更,需要发邮件通知
            } else {
                fillRow(kuduColumn, columnValue, operation.getRow());
//                row.addObject(kuduColumn.getName(), columnValue);
                if (!hasAddData) {
                    hasAddData = true;
                }
            }
        }

        if (hasAddData) {
            return operation;
        } else {
            throw new RuntimeException("no column value be set,please confirm the topics are match the kudu table: " + kuduTableName);
        }
    }

    /**
     * copy from org.apache.kudu.client.PartialRow.addObject(int, java.lang.Object)
     */
    private void fillRow(ColumnSchema kuduColumn, Object srcColumnValue, PartialRow row) {
        final String kuduColumnName = kuduColumn.getName();
        final Type kuduColumnType = kuduColumn.getType();
        if (srcColumnValue == null) {
            row.addObject(kuduColumnName, null);
            return;
        }

        final String value = String.valueOf(srcColumnValue);

        switch (kuduColumnType) {
            case BOOL:
                row.addBoolean(kuduColumnName, Boolean.parseBoolean(value));
                break;
            case INT8:
                row.addByte(kuduColumnName, Byte.parseByte(value));
                break;
            case INT16:
                row.addShort(kuduColumnName, Short.parseShort(value));
                break;
            case INT32:
                row.addInt(kuduColumnName, Integer.parseInt(value));
                break;
            case INT64:
                row.addLong(kuduColumnName, Long.parseLong(value));
                break;
            case UNIXTIME_MICROS:
//                TODO date类型转换会出现1970-01-01
                Timestamp timestamp;
                try {
                    timestamp = new Timestamp(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    try {
//                        logger.info("00000000000000: " + value);
                        final Date date = DateUtils.parseDate(value, datePatterns);
                        final String convertedDateStr = sdf.format(date);
//                        logger.info("11111111111111111: " + convertedDateStr);
                        final Date convertedDate = DateUtils.parseDate(convertedDateStr, datePatterns);
//                        logger.info("22222222222222222: " + convertedDate);
                        timestamp = new Timestamp(convertedDate.getTime());
//                        logger.info("33333333333333333: " + timestamp);
                    } catch (ParseException ex) {
                        throw new RuntimeException("parse date error:" + value);
                    }
                }

                row.addTimestamp(kuduColumnName, timestamp);
                break;
            case FLOAT:
                row.addFloat(kuduColumnName, Float.parseFloat(value));
                break;
            case DOUBLE:
                row.addDouble(kuduColumnName, Double.parseDouble(value));
                break;
            case STRING:
                row.addString(kuduColumnName, value);
                break;
            case BINARY:
                row.addBinary(kuduColumnName, value.getBytes(StandardCharsets.UTF_8));
                break;
            case DECIMAL:
                // required "decimal.handling.mode": "string"
                row.addDecimal(kuduColumnName, new BigDecimal(value));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + kuduColumnType);
        }
    }


    public void sync(List<Operation> operations) throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        if (operations.isEmpty()) {
            return;
        }

        for (Operation operation : operations) {
            session.apply(operation);
        }

        final List<OperationResponse> resps = session.flush();
        if (resps.size() > 0) {
            OperationResponse resp = resps.get(0);
            if (resp.hasRowError()) {
                throw new RuntimeException("sync to kudu error:" + resp.getRowError());
            }
        }
        watch.stop();
        if (logEnabled) {
            logger.info("sync: " + operations.size() + ",to " + kuduTableName + ",spend: " + watch);
        }
    }

    public void stop() throws KuduException {
        session.close();
        kuduClient.close();
    }

    @Override
    public String toString() {
        return "KuduSyncer{" +
                "masterAddresses='" + masterAddresses + '\'' +
                ", kuduTableName='" + kuduTableName + '\'' +
                ", maxBatchSize=" + maxBatchSize +
                ", onlySyncValueChangedColumns=" + onlySyncValueChangedColumns +
                ", logEnabled=" + logEnabled +
                ", kuduColumnNameMapKuduColumn=" + kuduColumnNameMapKuduColumn +
                '}';
    }
}
