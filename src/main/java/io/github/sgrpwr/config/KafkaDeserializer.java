package io.github.sgrpwr.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sgrpwr.dtos.KafkaRequestDto;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaDeserializer implements Deserializer<KafkaRequestDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public KafkaRequestDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, KafkaRequestDto.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize KafkaRequestDto", e);
        }
    }

    @Override
    public void close() {
    }
}
