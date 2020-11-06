import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class PartitionGeneratorTest {
    @Test
    public void test() throws ParseException {
//        CALL kudu.system.add_range_partition('schema', 'table', '{"lower": "2018-01-01", "upper": "2018-06-01"}')
        final TimeUnit timeUnit = TimeUnit.MONTH;
        final Date lowerDate = DateUtils.parseDate("2018-01-01", TimeUnit.patterns());
        final int amount = 1;
        if (amount < 0) {
            throw new RuntimeException("amount less than 0:" + amount);
        }
        final int partitionCount = 10;
        final String schema = "s1";
        final String table = "t1";
        Date prevDate = lowerDate, nextDate;
        final List<String> procedures = new ArrayList<>(partitionCount);
        String json;
        for (int i = 0; i < partitionCount; i++) {
            switch (timeUnit) {
                case DAY:
                    nextDate = DateUtils.addDays(prevDate, amount);
                    break;
                case MONTH:
                    nextDate = DateUtils.addMonths(prevDate, amount);
                    break;
                case YEAR:
                    nextDate = DateUtils.addYears(prevDate, amount);
                    break;
                default:
                    throw new RuntimeException("not supported:" + timeUnit);
            }
            json = "{\"lower\": \"" + DateFormatUtils.format(prevDate, timeUnit.pattern) + "\", \"upper\": \"" + DateFormatUtils.format(nextDate, timeUnit.pattern) + "\"}";
            procedures.add("CALL kudu.system.add_range_partition('" + schema + "', '" + table + "', '" + json + "')");
            prevDate = nextDate;
        }
        System.out.println(String.join("\n", procedures));
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
