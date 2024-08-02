package io.github.sgrpwr.controller;

import io.github.sgrpwr.DTO.KafkaRequestDto;
import io.github.sgrpwr.producer.MessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class MessageController {

    private MessageProducer messageProducer;

    @Autowired
    public MessageController(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
    }

    @CrossOrigin
    @RequestMapping(value = "/publish", method = {RequestMethod.POST})
    public ResponseEntity<String> publishTest(@RequestBody KafkaRequestDto message) {
        messageProducer.publishToQueue(message, null);
        return new ResponseEntity<>("Message sent", HttpStatus.OK);
    }

    /*@GetMapping(value = "/publish")
    public ResponseEntity<String> publishTest(@RequestParam("message") String message) {
        messageProducer.publishMessage(message);
        return new ResponseEntity<>("Message sent", HttpStatus.OK);
    }*/
}
