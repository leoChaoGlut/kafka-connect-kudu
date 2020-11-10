import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import personal.leo.kafka_connect_kudu.KuduSyncer;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
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
        final String dateStr = "2020-11-10 01:04:04";

        final Date date = DateUtils.parseDate(dateStr, KuduSyncer.datePatterns);
        final ZonedDateTime zonedDateTime = date.toInstant().atZone(ZoneId.of("UTC+1"));
        System.out.println(date.toInstant().atZone(ZoneId.of("UTC+1")));
        System.out.println(date.toInstant().atZone(ZoneId.of("UTC+2")));
        System.out.println(date.toInstant().atZone(ZoneId.of("UTC+3")));
        System.out.println(date.toInstant().atZone(ZoneId.of("UTC+8")));
        final Timestamp from = Timestamp.from(zonedDateTime.toInstant());

        System.out.println(from);
    }

    @Test
    public void test3() throws ParseException {
        String l = "2020-07-10T18:18:52Z";
        final StopWatch watch = StopWatch.createStarted();
        try {
            final long l1 = Long.parseLong(l);
        } catch (NumberFormatException e) {

        }
        watch.stop();
        System.out.println(watch);
    }

    @Test
    public void test4() throws ParseException {
        System.out.println(Locale.CHINA.toLanguageTag());
        System.out.println(Locale.CHINA);
        System.out.println(Locale.forLanguageTag(Locale.CHINA.toLanguageTag()));
    }
}
