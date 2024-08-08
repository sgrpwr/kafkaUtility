package io.github.sgrpwr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableKafka
public class KafkaApplication {

    public static void main(String[] args){
        SpringApplication.run(KafkaApplication.class, args);
        System.out.println("up and running...");
    }

}