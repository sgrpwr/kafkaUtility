package in.countrydelight.sagar01.config;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.countrydelight.sagar01.dtos.KafkaRequestDto;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class MessageSerializer implements Deserializer<KafkaRequestDto> {


    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public KafkaRequestDto deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, KafkaRequestDto.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
