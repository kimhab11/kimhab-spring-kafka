package com.kafka.kimhabspringkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    // Simple consumer
    @KafkaListener(topics = "user-events", groupId = "user-group")
    public void consumeUserEvents(String message) {
        log.info("Consumed user event: {}", message);
        // Process the message
    }

    // Consumer with full record details
    @KafkaListener(topics = "notifications", groupId = "notification-group")
    public void consumeNotifications(ConsumerRecord<String, String> record) {
        log.info("Consumed notification - Key: {}, Value: {}, Partition: {}, Offset: {}",
                record.key(), record.value(), record.partition(), record.offset());
    }

    // Consumer with headers and payload
    @KafkaListener(topics = "user-events", groupId = "analytics-group")
    public void consumeForAnalytics(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Analytics consumer - Message: {}, Partition: {}, Offset: {}",
                message, partition, offset);
    }

    // Manual acknowledgment consumer
    @KafkaListener(
            topics = "user-events",
            groupId = "manual-ack-group",
            containerFactory = "kafkaManualAckListenerContainerFactory"
    )
    public void consumeWithManualAck(String message, Acknowledgment acknowledgment) {
        try {
            log.info("Processing message: {}", message);
            // Process message

            // Manually commit offset
            acknowledgment.acknowledge();
            log.info("Message acknowledged");
        } catch (Exception e) {
            log.error("Error processing message", e);
            // Don't acknowledge - message will be reprocessed
        }
    }
}
