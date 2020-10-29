import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CommonTest {
    @Test
    public void test() {
        String regex = "^test[0-9]*\\.t7[0-9]+$";
        Pattern pattern = Pattern.compile(regex);
    }

    @Test
    public void test1() {
        String json = "{\n" +
                "  \"op\": \"u\",\n" +
                "  \"before\": {\n" +
                "    \"id\": 6,\n" +
                "    \"name\": \"aa\"\n" +
                "  },\n" +
                "  \"after\": {\n" +
                "    \"id\": 111\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"thread\": 62021,\n" +
                "    \"server_id\": 101,\n" +
                "    \"version\": \"1.3.0.Final\",\n" +
                "    \"file\": \"master.000003\",\n" +
                "    \"connector\": \"mysql\",\n" +
                "    \"pos\": 224583723,\n" +
                "    \"name\": \"hdp04\",\n" +
                "    \"row\": 0,\n" +
                "    \"ts_ms\": 1603870760000,\n" +
                "    \"snapshot\": \"false\",\n" +
                "    \"db\": \"test\",\n" +
                "    \"table\": \"t5\"\n" +
                "  },\n" +
                "  \"ts_ms\": 1603870760496\n" +
                "}";
        final JSONObject payload = JSON.parseObject(json);
        final Map<String, Object> before = payload.getObject("before1", Map.class);
        System.out.println(before);
    }

    @Test
    public void test2() {
        List<Integer> arr = new ArrayList<>();
        arr.add(1);
        System.out.println(arr.size());
        arr.add(2);
        System.out.println(arr.size());
        arr.clear();
        System.out.println(arr.size());
    }
}
