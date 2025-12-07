package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.metric.KafkaMetricsService;
import com.kafka.kimhabspringkafka.dto.OrderEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final KafkaMetricsService metricsService;

    /**
     * Consumer for processing orders
     * <p>
     * ┌─────────────────────────────────────────────────────┐
     * │ Kafka Topic: orders-topic                           │
     * │ Partition 0: [msg0][msg1][msg2][msg3][msg4]        │
     * │                           ↑                          │
     * │                      Consumer Offset                │
     * │                      (committed: 2)                  │
     * └─────────────────────────────────────────────────────┘
     */
    @KafkaListener(
            topics = "orders-topic",
            groupId = "order-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload OrderEvent order,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,  // @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, (Spring Kafka 3.x).
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        Timer.Sample sample = metricsService.startConsumerTimer();
        try {
            log.info("{ Received order: {} from topic: {}, partition: {}, offset: {}",order.getOrderId(), topic, partition, offset);

            // Process the order
            processOrder(order);

            // Record metrics
            metricsService.recordMessageConsumed();

            // Manual acknowledgment On success
            // Tells Kafka "I have successfully processed this message, mark it as consumed."
            acknowledgment.acknowledge();
            log.info("{ Order processed & committed(acknowledge) successfully: {}", order.getOrderId());

        } catch (Exception e) {
            metricsService.recordMessageFailed();
            log.error("{ Error processing order: {} , {}", order.getOrderId(), e.getMessage());
            // -> do not acknowledge -> Offset NOT committed -> message will be redelivered
            // Or implement your own retry logic here
        } finally {
            metricsService.recordConsumerProcessingTime(sample);
        }
    }

    private void processOrder(OrderEvent order) {

        try {
            // Business logic here
            log.info("{ Processing order: {} for customer: {} with amount: {}",
                    order.getOrderId(), order.getCustomerId(), order.getAmount());

            // === POSSIBLE REAL BUSINESS LOGIC (replaces Thread.sleep) ===
            // 1. Validate order
            // 2. Check inventory
            // 3. Process payment (may take time)
            // 4. Update database
            // 5. Send notification

            Thread.sleep(100); // Stimulate
        } catch (InterruptedException e) {
            // Application is shutting down gracefully
            Thread.currentThread().interrupt();
            log.error("{ InterruptedException, order id {}", order.getOrderId());
        } catch (Exception e){
            log.error("{ Failed to process order: {}", order.getOrderId(), e);
            // Optional: Send to DLQ after X retries
        }
    }
}
