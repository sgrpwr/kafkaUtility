package io.github.sgrpwr.consumer;

import io.github.sgrpwr.DTO.KafkaRequestDto;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.function.Function;

@Service
public class MessageConsumer {
    private List<Function<Object, Object>> runnables;

    @Retryable(
            value = { Exception.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, maxDelay = 3000)
    ) // specifying the maximum number of retry attempts (3) and the delay between retries (1 second with a maximum of 3 seconds).
    //@KafkaListener(topics = "${kafka.topic}", groupId = "xyz")
    private void consume(KafkaRequestDto message) {
        try {
            // Process the message here
            System.out.println("Received message: " + message);

            if (!CollectionUtils.isEmpty(runnables)) {
                runnables.forEach(r -> r.apply(message));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error processing message: " + message, e);
        }
    }

    public void listen(Function<Object, Object> function) {
        runnables.add(function);
    }
}
