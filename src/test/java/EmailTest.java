import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.junit.Test;
import personal.leo.kafka_connect_kudu.constants.PropDefaultValues;

import java.util.Arrays;
import java.util.stream.Collectors;

public class EmailTest {
    @Test
    public void test() throws EmailException {
        for (int i = 0; i < 3; i++) {
            Email email = new SimpleEmail();
            email.setHostName("smtp.163.com");
//        email.setSmtpPort(465);
            email.setSmtpPort(25);
            email.setAuthenticator(new DefaultAuthenticator("ypshengxian@163.com", "YBYFNAGEYXJAGQMO"));
//        email.setSSLOnConnect(true);
            email.setFrom("ypshengxian@163.com");
            email.setSubject("TestMail");
            email.setMsg("This is a test mail ... :-)");
            email.addTo("liaochao@ypshengxian.com");
            System.out.println(email);
            email.send();
            System.out.println(i);
        }

    }

    @Test
    public void test1() throws EmailException {
        System.out.println(Arrays.stream(StringUtils.splitByWholeSeparator(PropDefaultValues.emailTo, ",")).collect(Collectors.toList()));

    }
}
