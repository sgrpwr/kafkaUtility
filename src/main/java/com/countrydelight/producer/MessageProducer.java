package com.countrydelight.producer;

import com.countrydelight.DTO.KafkaRequestDto;
import com.countrydelight.utils.NewKafkaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.concurrent.TimeUnit;


@Service
public class MessageProducer{

    private final Logger logger =LoggerFactory.getLogger(MessageProducer.class);

    /*@Autowired
    private KafkaTemplate<String, KafkaBaseDto> template;*/

    @Autowired
    private NewKafkaUtil template;


    public NewKafkaUtil getTemplate() {
        return template;
    }


    public void setTemplate(NewKafkaUtil template) {
        this.template = template;
    }
  /*  @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;*/

    @Value("${kafka.topic}")
    private String defaultTopic;
 /*   public boolean publishMessage(String str) {
        kafkaTemplate.send("myTopic", str);
        return true;
    }
*/
    public boolean publishToQueue(KafkaRequestDto message, String topic) {
        try {
            if(Objects.isNull(template)) {
                logger.info("******************* kafka Template is null *****************");
                return false;
            }

            if(!StringUtils.hasLength(topic) && !StringUtils.hasLength(defaultTopic)) {
                logger.info("******************* kafka topic is null from env *****************");
                return false;
            }

            if(Objects.isNull(message)) {
                logger.info("*******************kafka Message Dto is null*****************");
                return false;
            }
            else if(Objects.isNull(message.getAnalyticsType())) {
                logger.info("*******************kafka Message Key -Analytics Type is null*****************");
                return false;
            }
            else if(Objects.isNull(message.getBody())) {
                logger.info("*******************kafka Message Body - Payload  is null*****************");
                return false;
            }

            logger.info("Message send : {}", message);

            template.send(StringUtils.hasLength(topic) ? topic : defaultTopic, message).get(10, TimeUnit.SECONDS);

            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }
}
