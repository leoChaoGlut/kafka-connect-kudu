import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SourceRequestBodyBuilder {
    @Before
    public void before() {

    }

    @Test
    public void source() {
        final String databaseServerName = "db";
        final String suffix = databaseServerName.length() > 4 ? databaseServerName.substring(databaseServerName.length() - 4) : databaseServerName;
        final String connectName = "src-dbz-" + suffix;
        final List<String> databaseIncludeRegexList = Arrays.asList(
                "s1",
                "s2"
        );
        final List<String> tableIdIncludeRegexList = Arrays.asList(
                "s1.t1",
                "s1.t2",
                "s2.t3"
        );

        final String databaseIncludeRegex = buildDatabaseIncludeRegex(databaseIncludeRegexList);
        final String tableIdIncludeRegex = buildTableIdIncludeRegex(tableIdIncludeRegexList);

        JSONObject config = new JSONObject();
        config.put("database.server.id", "1001");
        config.put("database.hostname", "host");
        config.put("database.user", "user");
        config.put("database.password", "password");
        config.put("database.server.name", databaseServerName);
        config.put("database.include.list", databaseIncludeRegex);
        config.put("table.include.list", tableIdIncludeRegex);
        config.put("database.history.kafka.topic", "history." + connectName);
        config.put("database.port", "3306");

//        don't mod
        config.put("decimal.handling.mode", "string");
        config.put("database.serverTimezone", "UTC");
        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        config.put("database.history.kafka.bootstrap.servers", "mq1:10001,mq2:10001,mq3:10001");

        JSONObject requestJson = new JSONObject();
        requestJson.put("name", connectName);
        requestJson.put("config", config);

        System.out.println(requestJson.toJSONString());
    }


    private String buildTableIdIncludeRegex(List<String> tableIdIncludeRegexList) {
        return tableIdIncludeRegexList.stream()
                .map(str -> StringUtils.replace(str, ".", "\\."))
                .collect(Collectors.joining(","));
    }

    private String buildDatabaseIncludeRegex(List<String> databaseIncludeRegexList) {
        return databaseIncludeRegexList.stream()
                .map(str -> StringUtils.replace(str, ".", "\\."))
                .collect(Collectors.joining(","));
    }


}
