package com.countrydelight.utils;

import com.countrydelight.DTO.KafkaRequestDto;
import com.countrydelight.common.SyncServerUrls;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
//@DependsOn("syncServerUrls")
public class NewKafkaUtil {

    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.host:localhost}")
    private String host;

    @Value("${kafka.port:9092}")
    private String port;

    private String KAFKA_BROKER = SyncServerUrls.KAFKA_BOOTSTRAP_SERVER;

    @PostConstruct
    private void init() {
        this.kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigurations()));
    }

    private Map<String, Object> producerConfigurations() {
        KAFKA_BROKER = SyncServerUrls.KAFKA_BOOTSTRAP_SERVER;
        Map<String, Object> configurations = new HashMap<>();
        configurations.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"10000000");
        if(Objects.nonNull(KAFKA_BROKER))
            configurations.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        else{
            String fallbackBroker = host + ":" + port;
            //String fallbackBroker = "localhost:9092";
            configurations.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, fallbackBroker);
            System.out.println(fallbackBroker);
        }
        configurations.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configurations.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class);
        return configurations;
    }

    public ListenableFuture<SendResult<String, Object>> send(String topic, KafkaRequestDto message) {
        return kafkaTemplate.send(topic, message);
    }
}

