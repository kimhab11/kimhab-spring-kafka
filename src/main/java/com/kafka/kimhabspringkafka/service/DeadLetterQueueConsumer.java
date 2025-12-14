package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.dto.DLQMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeadLetterQueueConsumer {
    /**
     * Consumer for Dead Letter Queue
     * Monitors failed messages for manual intervention or alerting
     */
    @KafkaListener(
            topics = "orders-topic-dlq",
            groupId = "dlq-monitoring-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDLQ(
            @Payload DLQMessage dlqMessage,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.error("=== DEAD LETTER QUEUE MESSAGE RECEIVED ===");
        log.error("Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        log.error("Original Message: {}", dlqMessage.getOriginalMessage());
        log.error("Error Reason: {}", dlqMessage.getErrorReason());
        log.error("Failed At: {}", dlqMessage.getFailedAt());
        log.error("Retry Count: {}", dlqMessage.getRetryCount());
        log.error("=========================================");

        // Actions you can take:
        // 1. Store in database for manual review
        // 2. Send alert to monitoring system (PagerDuty, Slack, etc.)
        // 3. Attempt manual reprocessing after fixing the issue
        // 4. Generate reports for failed messages

        // Example: Store in database
        storeDLQMessageInDatabase(dlqMessage);

        // Example: Send alert
        sendAlertToMonitoring(dlqMessage);
    }

    private void storeDLQMessageInDatabase(DLQMessage dlqMessage) {
        // TODO: Implement database storage
        log.info("{ Storing DLQ message in database for order: {}",
                dlqMessage.getOriginalMessage().getOrderId());
    }

    private void sendAlertToMonitoring(DLQMessage dlqMessage) {
        // TODO: Implement alerting (Slack, Email, PagerDuty, etc.)
        log.warn("{ Sending alert for failed order: {}",
                dlqMessage.getOriginalMessage().getOrderId());
    }


    // another dlq test
    @KafkaListener(topics = "orders.DLT", groupId = "orders-dlt-processor")
    public void dltListen(ConsumerRecord<String, Object> record) {
        // Inspect headers for original exception and attempt count
        // Example: record.headers().lastHeader("kafka_retry_count")
        // Log, persist, or trigger manual reprocessing workflow
        System.out.println("DLT record: " + record.value());
    }
}
