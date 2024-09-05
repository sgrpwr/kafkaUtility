package in.countrydelight.sagar01.consumer;

import in.countrydelight.sagar01.common.DynamicBusinessLogicService;
import in.countrydelight.sagar01.config.KafkaDeserializer;
import in.countrydelight.sagar01.dtos.KafkaRequestDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
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

    @Value("${kafka.topics2}")
    private String[] topics2;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.group-id2}")
    private String groupId2;

    @Autowired
    public KafkaConsumerService(DynamicBusinessLogicService dynamicBusinessLogicService) {
        this.dynamicBusinessLogicService = dynamicBusinessLogicService;
    }

    @KafkaListener(topics = {"topic1", "topic2", "topic3"}, groupId = "${kafka.consumer.group-id}",
            topicPartitions = {
            @TopicPartition(topic = "topic1", partitions = {"0"}),
            @TopicPartition(topic = "topic2", partitions = {"0", "1"}),
            @TopicPartition(topic = "topic3", partitions = {"0"})})
    public void listen1(KafkaRequestDto kafkaRequestDto) {
        try {
            if (ObjectUtils.isEmpty(kafkaRequestDto)) {
                logger.info("******************* Inside listen 1 *****************");
                return;
            }
            logger.info("Received Message: " + kafkaRequestDto.getBody());
            System.out.println(kafkaRequestDto.getBody());

            dynamicBusinessLogicService.processMessage(kafkaRequestDto);
        } catch (Exception e) {
            logger.error("Error processing message: ", e);
        }
    }

    @KafkaListener(topics = {"topic1", "topic2", "topic3"}, groupId = "${kafka.consumer.group-id}",
            topicPartitions = {
                    @TopicPartition(topic = "topic1", partitions = {"2"}),
                    @TopicPartition(topic = "topic2", partitions = {"3"}),
                    @TopicPartition(topic = "topic3", partitions = {"2","3"})})
    public void listen2(KafkaRequestDto kafkaRequestDto) {
        try {
            if (ObjectUtils.isEmpty(kafkaRequestDto)) {
                logger.info("******************* Inside listen 2 *****************");
                return;
            }
            logger.info("Received Message: " + kafkaRequestDto.getBody());
            System.out.println(kafkaRequestDto.getBody());

            dynamicBusinessLogicService.processMessage(kafkaRequestDto);
        } catch (Exception e) {
            logger.error("Error processing message: ", e);
        }
    }


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
