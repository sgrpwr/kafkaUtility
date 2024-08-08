package io.github.sgrpwr.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sgrpwr.common.DynamicBusinessLogicService;
import io.github.sgrpwr.config.KafkaDeserializer;
import io.github.sgrpwr.dtos.KafkaRequestDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final DynamicBusinessLogicService dynamicBusinessLogicService;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics}")
    private String[] topics;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Autowired
    public KafkaConsumerService(DynamicBusinessLogicService dynamicBusinessLogicService) {
        this.dynamicBusinessLogicService = dynamicBusinessLogicService;
    }

    @KafkaListener(topics = "${kafka.topics}", groupId = "${kafka.consumer.group-id}")
    public void listen(String record) {
        try {
            KafkaRequestDto kafkaRequestDto = deserializeJsonToObject(record, KafkaRequestDto.class);
            if (ObjectUtils.isEmpty(kafkaRequestDto)) {
                logger.info("******************* Received empty message *****************");
                return;
            }

            logger.info("Received Message: " + kafkaRequestDto.getBody());
            System.out.println(kafkaRequestDto.getBody());

            dynamicBusinessLogicService.processMessage(record, kafkaRequestDto);
        } catch (Exception e) {
            logger.error("Error processing message: ", e);
        }
    }

    private <T> T deserializeJsonToObject(String json, Class<T> clazz) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON to object", e);
        }
    }

    /* @Bean
    public ConsumerAwareListenerErrorHandler consumerErrorHandler() {
        return (message, exception, consumer) -> {
            logger.error("Error while processing: " + message.getPayload() + ", exception: " + exception.getMessage());
            return null;
        };
    }*/

    public String consumeMessages(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, KafkaRequestDto> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            StringBuilder messages = new StringBuilder();

            // Poll for messages
            ConsumerRecords<String, KafkaRequestDto> records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                messages.append("Offset: ").append(record.offset())
                        .append(", Key: ").append(record.key())
                        .append(", Value: ").append(record.value())
                        .append("\n");
            });

            return messages.toString();
        }
    }

    }
