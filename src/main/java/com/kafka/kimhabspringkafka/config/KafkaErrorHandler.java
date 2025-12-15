package com.kafka.kimhabspringkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaErrorHandler implements CommonErrorHandler {
    @Override
    public void handleOtherException(
            Exception exception,
            Consumer<?, ?> consumer,
            MessageListenerContainer container,
            boolean batchListener
    ) {
        log.error("Kafka consumer error", exception);
        // Add custom error handling logic (e.g., send to DLQ)
    }
}
