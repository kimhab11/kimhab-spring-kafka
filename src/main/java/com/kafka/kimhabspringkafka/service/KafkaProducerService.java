package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.metric.KafkaMetricsService;
import com.kafka.kimhabspringkafka.dto.OrderEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final DeadLetterQueueService dlqService;

    private final KafkaMetricsService metricsService;

    private static final String MAIN_TOPIC = "orders-topic";

    /**
     * Send message with retry mechanism
     * Retries 3 times with exponential backoff
     * Synchronous send with callback
     */
    @Retryable(
            value = {Exception.class},
            maxAttempts = 3,
            backoff = @Backoff(
                    delay = 1000,      // Initial delay: 1 second
                    multiplier = 2.0,  // Exponential backoff
                    maxDelay = 10000   // Max delay: 10 seconds
            )
    )

    public void sendOrder(OrderEvent order) {
        Timer.Sample sample = metricsService.startProducerTimer();

        try {
            log.info("{ Attempting to send order: {}", order.toString());

            // send (topic, key , message)
            ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(MAIN_TOPIC, order.getOrderId(), order);
            future.addCallback(
                    result -> {
                        metricsService.recordMessageProduced();
                        metricsService.recordProducerLatency(sample);

                        log.info("{ Order sent successfully: {} to - topic:  {}, partition: {}, offset: {}",
                                order.getOrderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    },
                    ex -> {
                        metricsService.recordMessageFailed();
                        metricsService.recordProducerLatency(sample);

                        log.error("{ Failed to send order: [{}] due to {}", order, ex);
                        throw new RuntimeException("Kafka send failed", ex);
                    }
            );

            // Wait for result to enable retry
            future.get();  // blocks until send completes

        } catch (Exception e) {
            log.error("{ Error sending order to Kafka: {}", order.getOrderId(), e);
            throw new RuntimeException("{ Error sending order to Kafka, " +e.getMessage(), e); // rethrow to trigger retry
        }

//        try {
//            log.info("Attempting to send order: {}", order.getOrderId());
//
//            CompletableFuture<SendResult<String, Object>> future =
//                    kafkaTemplate.send(MAIN_TOPIC, order.getOrderId(), order);
//
//            future.whenComplete((result, ex) -> {
//                if (ex == null) {
//                    log.info("Order sent successfully: {} to partition: {}",
//                            order.getOrderId(),
//                            result.getRecordMetadata().partition());
//                } else {
//                    log.error("Failed to send order: {}", order.getOrderId(), ex);
//                    throw new RuntimeException("Kafka send failed", ex);
//                }
//            });
//
//            // Wait for result to enable retry
//            future.join();
//
//        } catch (Exception e) {
//            log.error("Error sending order to Kafka: {}", order.getOrderId(), e);
//            throw e; // Rethrow to trigger retry
//        }
    }

    /**
     * Recovery method - called after all retries exhausted
     * Sends failed message to Dead Letter Queue
     */
    @Recover
    public void recoverFromSendFailure(Exception e, OrderEvent order) {
        log.error("{ All retry attempts exhausted for order: {}. Sending to DLQ",order.getOrderId(), e);

        try {
            dlqService.sendToDLQ(order, e.getMessage());
        } catch (Exception dlqException) {
            log.error("{ Failed to send to DLQ. Manual intervention required for order: {}",
                    order.getOrderId(), dlqException);
            // In production, you might want to:
            // - Store in database for manual processing
            // - Send alert to monitoring system
            // - Write to file system as last resort
        }
    }

    /**
     * Synchronous send with explicit error handling
     */
    public void sendOrderSync(OrderEvent order) throws Exception {
        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(MAIN_TOPIC, order.getOrderId(), order)
                    .get(); // Blocking call

            log.info("{ Order sent successfully (sync): {}", order.getOrderId());

        } catch (Exception e) {
            log.error("{ Failed to send order synchronously: {}", order.getOrderId(), e);
            throw e;
        }
    }
}
