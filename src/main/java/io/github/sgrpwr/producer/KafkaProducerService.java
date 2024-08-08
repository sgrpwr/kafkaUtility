package io.github.sgrpwr.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sgrpwr.common.Constants;
import io.github.sgrpwr.dtos.KafkaRequestDto;
import io.github.sgrpwr.exceptions.CustomKafkaException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProducerFactory<String, String> producerFactory;

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate, ProducerFactory<String, String> producerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.producerFactory = producerFactory;
    }

    public ResponseEntity<String> sendMessage(KafkaRequestDto kafkaRequestDto) {
        try {
            if (Objects.isNull(kafkaTemplate)) {
                logger.info("Kafka Template is null");
                throw new CustomKafkaException("Kafka Template is null", CustomKafkaException.ErrorType.TEMPLATE_NULL);
            }

            if (!topicExists(kafkaRequestDto.getAnalyticsType())) {
                logger.info("Kafka Topic does not exist");
                throw new CustomKafkaException("Kafka Topic does not exist", CustomKafkaException.ErrorType.TOPIC_NOT_FOUND);
            }

            if (ObjectUtils.isEmpty(kafkaRequestDto.getBody())) {
                logger.info("Message is null or empty");
                throw new CustomKafkaException("Message is null or empty", CustomKafkaException.ErrorType.MESSAGE_EMPTY);
            }

            String serializedBody = serializeObjectToJson(kafkaRequestDto);

            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                    kafkaRequestDto.getAnalyticsType(), serializedBody);

            future.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    logger.info("Sent message with offset=[" + result.getRecordMetadata().offset() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    logger.error("Unable to send message due to : " + ex.getMessage());
                }
            });

            return new ResponseEntity<>(Constants.SUCCESS, HttpStatus.OK);
        } catch (RuntimeException e) {
            logger.error("RuntimeException occurred while sending message", e);
            throw e;
        }
    }
    private String serializeObjectToJson(KafkaRequestDto kafkaRequestDto) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(kafkaRequestDto);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    private boolean topicExists(String topic) {
        try (AdminClient adminClient = KafkaAdminClient.create(producerFactory.getConfigurationProperties())) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topic));
            KafkaFuture<Map<String, TopicDescription>> future = result.all();
            future.get();
            return true;
        } catch (TopicExistsException e) {
            return true;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception occurred while checking topic existence", e);
            return false;
        }
    }
}
