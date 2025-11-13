package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.model.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Simple message send
    public void sendMessage(String topic, String message){
        kafkaTemplate.send(topic, message);
        log.info("Sent message: {} to topic: {}", message, topic);
    }

    // Send with key (for partitioning)
    public void sendMessageWithKey(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
        log.info("Sent message: {}, with key: {} to topic: {}", message, key, topic);
    }

    // Send with callback
    public void sendMessageAsync(String topic, String message) {
        ListenableFuture<SendResult<String, String>> future =  kafkaTemplate.send(topic, message);

        future.addCallback(
                result -> log.info("Message [{}]sent: offset={}, partition={}",message,
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().partition()),
                ex -> log.error("Failed to send message", ex)
        );
    }

    // Send user event
    public void sendUserEvent(UserEvent event) {
        String message = String.format("%s|%s|%d",
                event.getUserId(), event.getAction(), event.getTimestamp());
        kafkaTemplate.send("user-events", event.getUserId(), message);
        log.info("Sent user event: {}", event);
    }
}
