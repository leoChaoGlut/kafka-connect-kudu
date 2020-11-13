import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.*;

public class PartitionProcedureBuilder {
    Statement statement;

    @Before
    public void before() throws SQLException {
        String url = "jdbc:presto://etl01:10100/kudu";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        Connection connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
    }

    @Test
    public void test() throws ParseException {
//        CALL kudu.system.add_range_partition('schema', 'table', '{"lower": "2018-01-01", "upper": "2018-06-01"}')
        final TimeUnit timeUnit = TimeUnit.DAY;
        final Date lowerDate = DateUtils.parseDate("2020-09-01", TimeUnit.patterns());
        final int step = 30;
        if (step < 0) {
            throw new RuntimeException("step less than 0:" + step);
        }
        final int partitionCount = Integer.MAX_VALUE;
        final String schema = "ods_wms";
        final String table = "out_package_detail";
        Date prevDate = lowerDate, nextDate, upperDate, now = new Date();

        switch (timeUnit) {
            case DAY:
                upperDate = DateUtils.addDays(now, 30);
                break;
            case MONTH:
                upperDate = DateUtils.addMonths(now, 2);
                break;
            case YEAR:
                upperDate = DateUtils.addYears(now, 1);
                break;
            default:
                throw new RuntimeException("not supported:" + timeUnit);
        }

        final List<String> procedures = new ArrayList<>();
        String json;
        for (int i = 0; i < partitionCount; i++) {
            switch (timeUnit) {
                case DAY:
                    nextDate = DateUtils.addDays(prevDate, step);
                    break;
                case MONTH:
                    nextDate = DateUtils.addMonths(prevDate, step);
                    break;
                case YEAR:
                    nextDate = DateUtils.addYears(prevDate, step);
                    break;
                default:
                    throw new RuntimeException("not supported:" + timeUnit);
            }
            json = "{\"lower\": \"" + DateFormatUtils.format(prevDate, timeUnit.pattern) + "\", \"upper\": \"" + DateFormatUtils.format(nextDate, timeUnit.pattern) + "\"}";
            final String procedure = "CALL kudu.system.add_range_partition('" + schema + "', '" + table + "', '" + json + "');";
            System.out.println(procedure);
            procedures.add(procedure);
            prevDate = nextDate;
            if (nextDate.after(upperDate)) {
                break;
            }
        }
//        System.out.println(String.join("\n", procedures));

//        procedures.forEach(this::execute);
    }

    private void execute(String sql) {
        try {
            System.out.println(sql);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public enum TimeUnit {
        DAY("yyyy-MM-dd"),
        MONTH("yyyy-MM"),
        YEAR("yyyy"),
        ;
        public String pattern;

        TimeUnit(String pattern) {
            this.pattern = pattern;
        }

        public static String[] patterns() {
            return Arrays.stream(values())
                    .map(timeUnit -> timeUnit.pattern)
                    .toArray(String[]::new);
        }
    }

}
