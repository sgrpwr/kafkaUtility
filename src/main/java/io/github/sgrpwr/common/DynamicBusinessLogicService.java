package io.github.sgrpwr.common;

import io.github.sgrpwr.dtos.KafkaRequestDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DynamicBusinessLogicService {

    private static final Logger logger = LoggerFactory.getLogger(DynamicBusinessLogicService.class);

    /**
     * Process the Kafka message based on dynamic business logic.
     *
     * @param topic The Kafka topic from which the message was received.
     * @param kafkaRequestDto The Kafka request data transfer object containing message details.
     */
    public void processMessage(String topic, KafkaRequestDto kafkaRequestDto) {
        String analyticsType = kafkaRequestDto.getAnalyticsType();
        String sourceType = kafkaRequestDto.getSourceType();

        String body = null;
        if (kafkaRequestDto.getBody() instanceof String) {
            body = (String) kafkaRequestDto.getBody();
        } else {
            logger.warn("Expected body to be of type String, but got: " + kafkaRequestDto.getBody().getClass().getName());
        }

        // Implement dynamic business logic based on the topic and KafkaRequestDto
        logger.info("Processing message from topic: " + topic);
        logger.info("Analytics Type: " + analyticsType);
        logger.info("Source Type: " + sourceType);
        logger.info("Message Body: " + body);

        // Example: Forward message to another microservice
        // restTemplate.postForEntity("http://another-microservice/endpoint", kafkaRequestDto, String.class);
    }
}
