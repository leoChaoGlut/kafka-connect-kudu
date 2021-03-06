package personal.leo.kafka_connect_kudu.converter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class JSONObjectConverter implements Converter {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("isKey:" + isKey + ", configure: " + configs);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return JSON.toJSONBytes(value);
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            final JSONObject jsonObject = value == null ? null : JSON.parseObject(new String(value, StandardCharsets.UTF_8));
            return new SchemaAndValue(new SchemaBuilder(Schema.Type.STRUCT).build(), jsonObject);
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize string: ", e);
        }
    }


}
