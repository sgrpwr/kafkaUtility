package io.github.sgrpwr.controller;

import io.github.sgrpwr.consumer.KafkaConsumerService;
import io.github.sgrpwr.dtos.KafkaRequestDto;
import io.github.sgrpwr.producer.KafkaProducerService;
import io.github.sgrpwr.consumer.KafkaConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class MessageController {

    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;

    @Autowired
    public MessageController(KafkaProducerService kafkaProducerService, KafkaConsumerService kafkaConsumerService) {
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaConsumerService = kafkaConsumerService;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> sendMessage(@RequestBody KafkaRequestDto kafkaRequestDto) {
        try {
            kafkaProducerService.sendMessage(kafkaRequestDto);
            return new ResponseEntity<>("Message sent successfully", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("Failed to send message: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/consume")
    public ResponseEntity<String> consumeMessages(@RequestParam String topic) {
        try {
            String messages = kafkaConsumerService.consumeMessages(topic);
            return new ResponseEntity<>(messages, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("Failed to consume messages: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<String> getStatus() {
        return new ResponseEntity<>("Kafka application is up and running", HttpStatus.OK);
    }
}
