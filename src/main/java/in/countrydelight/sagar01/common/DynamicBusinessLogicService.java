package in.countrydelight.sagar01.common;

import in.countrydelight.sagar01.dtos.KafkaRequestDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

@Service
public class DynamicBusinessLogicService {

    private static final Logger logger = LoggerFactory.getLogger(DynamicBusinessLogicService.class);

    public void processMessage(KafkaRequestDto kafkaRequestDto) {

        if (!ObjectUtils.isEmpty(kafkaRequestDto)) {
            logger.info("Topic Name: {}, Source Type: : {}, Message Body: {}", kafkaRequestDto.getTopicName(), kafkaRequestDto.getSourceType(), kafkaRequestDto.getBody());
        } else {
            logger.warn("Expected body to be of type String, but got: " + kafkaRequestDto.getBody().getClass().getName());
        }

        // restTemplate.postForEntity("http://another-microservice/endpoint", kafkaRequestDto, String.class);
    }
}
