package io.github.sgrpwr.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaListenerProperties {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics}")
    private String[] topics;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String[] getTopics() {
        return topics;
    }

    public String getGroupId() {
        return groupId;
    }
}
