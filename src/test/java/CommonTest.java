import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import personal.leo.kafka_connect_kudu.KuduSyncer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class CommonTest {
    @Test
    public void test() {
        String regex = "^(newretail)\\.(nr_pos_payment_slip|nr_pos_payment_slip_item)$";
        Pattern pattern = Pattern.compile(regex);
        int a = 1112223334;
    }

    @Test
    public void test1() {
        String json = "{\"before\":null,\"after\":{\"id\":1313871605318836225,\"org_id\":820,\"create_time\":1602115176000,\"creator_id\":17671723459,\"creator_name\":\"系统缴款\",\"update_time\":1602115176000,\"updator_id\":3604656,\"updator_name\":\"17671723459\",\"tenant_id\":null,\"bill_no\":\"POS003582020100750\",\"type\":\"SYSTEM\",\"store_id\":820,\"store_name\":\"武汉汀澜公馆店\",\"pos_id\":1300398693479882754,\"pos_code\":\"POS00358\",\"start_time\":1602107955000,\"total\":\"6.80\",\"difference\":\"0.00\",\"uploaded\":false},\"source\":{\"version\":\"1.3.0.Final\",\"connector\":\"mysql\",\"name\":\"uf6026for2zz2o3dt\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"newretail\",\"table\":\"nr_pos_payment_slip\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.001644\",\"pos\":207806833,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1604729168418,\"transaction\":null}";
        final JSONObject payload = JSON.parseObject(json);
        final Map<String, Object> before = payload.getObject("before1", Map.class);
        final Map<String, Object> before1 = payload.getObject("after", Map.class);
        System.out.println(JSON.toJSONString(payload.getInnerMap()));
        System.out.println(before);
        System.out.println(before1);
    }

    @Test
    public void test11() {
        String json = "{\"SHOP_NAME2\":\"shop1\",\"SUM_PAY_PRICE\":3.5999999999999996}";
        final Map<String, Object> dataSet = JSON.parseObject(json, Map.class);
        System.out.println(dataSet);
        for (Map.Entry<String, Object> entry : dataSet.entrySet()) {
            final String columnName = entry.getKey();
            final Object columnValue = entry.getValue();
        }
    }

    @Test
    public void test2() throws ParseException {
        final String dateStr = "2020-11-10T12:17:03Z";
        final Date date = DateUtils.parseDate(dateStr, KuduSyncer.datePatterns);
        final Instant instant = date.toInstant().atZone(ZoneId.of("GMT+3")).toInstant();
        System.out.println(Timestamp.from(instant));
    }

    @Test
    public void test22() throws ParseException {
        SimpleDateFormat sdf8 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf8.setTimeZone(TimeZone.getTimeZone("GMT+16"));
        final StopWatch watch = StopWatch.createStarted();
        final String dateStr = "2020-11-10T12:17:03Z";
        final Date date = DateUtils.parseDate(dateStr, KuduSyncer.datePatterns);
        final String dateStr2 = sdf8.format(date);
        final Date date2 = DateUtils.parseDate(dateStr2, KuduSyncer.datePatterns);
        System.out.println(date2);
        watch.stop();
        System.out.println(watch);
    }

    @Test
    public void test3() throws ParseException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            final Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }

    @Test
    public void test4() throws ParseException {
        System.out.println(Locale.CHINA.toLanguageTag());
        System.out.println(Locale.CHINA);
        System.out.println(Locale.forLanguageTag(Locale.CHINA.toLanguageTag()));
    }

    @Test
    public void test5() throws ParseException, IOException {
        final String json = IOUtils.toString(CommonTest.class.getResourceAsStream("test.json"), StandardCharsets.UTF_8);
        final List<JSONObject> jsonObjects = JSON.parseArray(json, JSONObject.class);
        for (JSONObject jsonObject : jsonObjects) {
            final JSONObject after = jsonObject.getJSONObject("after");
            System.out.println(after.get("id"));
        }
    }
}
