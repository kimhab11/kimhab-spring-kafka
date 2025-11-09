package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class Sender {
    private final KafkaTemplate<String, Message> kafkaTemplate;

    public Sender(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, Message message) {
        log.info("Kafka Sender. Topic: {}, Message: {}", topic, message);
        kafkaTemplate.send(topic, message);
    }
}
