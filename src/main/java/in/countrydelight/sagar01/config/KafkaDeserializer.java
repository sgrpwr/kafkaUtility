package in.countrydelight.sagar01.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.countrydelight.sagar01.dtos.KafkaRequestDto;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaDeserializer implements Deserializer<KafkaRequestDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuration is not needed for this deserializer
    }

    @Override
    public KafkaRequestDto deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, KafkaRequestDto.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON to KafkaRequestDto", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
