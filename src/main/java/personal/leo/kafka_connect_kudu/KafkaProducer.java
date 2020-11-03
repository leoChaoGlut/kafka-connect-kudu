package personal.leo.kafka_connect_kudu;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

public class KafkaProducer {
    private final Map<String, String> config;
    private final String kuduTableName;
    private final Producer<String, String> producer;

    public KafkaProducer(Map<String, String> config) {
        this.config = config;
        this.kuduTableName = config.get(PropKeys.kuduTableName);

        final String kafkaBrokers = config.get(PropKeys.kafkaBrokers);
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public void send(String value) {
        producer.send(new ProducerRecord<>(kuduTableName, value));
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public Producer<String, String> getProducer() {
        return producer;
    }
}
