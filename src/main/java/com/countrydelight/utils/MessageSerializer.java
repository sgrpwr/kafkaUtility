package com.countrydelight.utils;

import com.countrydelight.DTO.KafkaRequestDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

@Component
public class MessageSerializer implements Serializer<KafkaRequestDto> {

    @Override
    public byte[] serialize(String topic, KafkaRequestDto data) {
        byte[] serializedValue = null;
        ObjectMapper om = new ObjectMapper();
        if (data != null) {
            try {
                serializedValue = om.writeValueAsString(data).getBytes();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        return serializedValue;
    }
}
