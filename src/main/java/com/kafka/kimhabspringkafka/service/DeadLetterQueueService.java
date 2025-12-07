package com.kafka.kimhabspringkafka.service;

import com.kafka.kimhabspringkafka.dto.DLQMessage;
import com.kafka.kimhabspringkafka.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class DeadLetterQueueService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String DLQ_TOPIC = "orders-topic-dlq";

    /**
     * Send failed message to Dead Letter Queue with metadata
     */
    public void sendToDLQ(OrderEvent originalMessage, String errorReason) {
        try {
            DLQMessage dlqMessage = DLQMessage.builder()
                    .originalMessage(originalMessage)
                    .errorReason(errorReason)
                    .failedAt(LocalDateTime.now())
                    .originalTopic("orders-topic")
                    .retryCount(3) // Number of retries attempted
                    .build();

//            kafkaTemplate.send(DLQ_TOPIC, originalMessage.getOrderId(), dlqMessage)
//                    .whenComplete((result, ex) -> {
//                        if (ex == null) {
//                            log.info("Message sent to DLQ successfully: {}",
//                                    originalMessage.getOrderId());
//                        } else {
//                            log.error("Failed to send to DLQ: {}",
//                                    originalMessage.getOrderId(), ex);
//                        }
//                    });

              // This version 2.7.* use with ListenableFuture
//            ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(DLQ_TOPIC, originalMessage.getOrderId(), dlqMessage);
//            future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
//                @Override
//                public void onFailure(Throwable ex) {
//                    log.error("{ Failed to send to DLQ: {}", originalMessage.getOrderId(), ex);
//                }
//
//                @Override
//                public void onSuccess(SendResult<String, Object> result) {
//                    log.info("{ Message sent to DLQ successfully: {}", originalMessage.getOrderId());
//                }
//            });

            kafkaTemplate.send(DLQ_TOPIC, originalMessage.getOrderId(), dlqMessage)
                    .addCallback(
                            result -> log.info("{ Message sent to DLQ successfully: {}", originalMessage.getOrderId()),
                            ex -> log.error("{ Failed to send to DLQ: {}", originalMessage.getOrderId(), ex)
                    );

        } catch (Exception e) {
            log.error("{ Exception while sending to DLQ", e);
            throw new RuntimeException("{ DLQ send failed", e);
        }
    }

    /**
     * Send message to DLQ with custom retry count
     */
    public void sendToDLQ(OrderEvent originalMessage, String errorReason, int retryCount) {
        DLQMessage dlqMessage = DLQMessage.builder()
                .originalMessage(originalMessage)
                .errorReason(errorReason)
                .failedAt(LocalDateTime.now())
                .originalTopic("orders-topic")
                .retryCount(retryCount)
                .build();

        kafkaTemplate.send(DLQ_TOPIC, originalMessage.getOrderId(), dlqMessage);
    }
}
