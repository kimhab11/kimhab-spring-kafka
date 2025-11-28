package com.kafka.kimhabspringkafka;
import com.kafka.kimhabspringkafka.dto.OrderEvent;
import com.kafka.kimhabspringkafka.service.DeadLetterQueueService;
import com.kafka.kimhabspringkafka.service.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
//@DirtiesContext
//@EmbeddedKafka(
//        partitions = 1,
//        brokerProperties = {
//                "listeners=PLAINTEXT://localhost:9093",
//                "port=9093"
//        },
//        topics = {"orders-topic", "orders-topic-dlq"}
//)
@ExtendWith(MockitoExtension.class)
class KafkaIntegrationTest {

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockBean
    private DeadLetterQueueService dlqService;

    @Autowired
    private KafkaProducerService producerService;

    // ========== INTEGRATION TEST WITH RETRY ==========

    @Test
    void testRetryMechanism_ThreeAttempts() {
       // Arrange
        OrderEvent order = OrderEvent.builder()
                .orderId("RETRY-TEST-001")
                .build();

        SettableListenableFuture<SendResult<String, Object>> future = new SettableListenableFuture<>();
        future.setException(new RuntimeException("Kafka failure"));

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Act
        try {
            producerService.sendOrder(order);
        } catch (Exception e) {
            // Expected
        }

        // Assert - should retry 3 times
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(kafkaTemplate, times(3))
                            .send(anyString(), eq("RETRY-TEST-001"), eq(order));
                    verify(dlqService, times(1))
                            .sendToDLQ(eq(order), anyString());
                });
    }

    @Test
    void testRetryMechanism_SuccessOnSecondAttempt() throws Exception {

        // Arrange
        OrderEvent order = OrderEvent.builder()
                .orderId("RETRY-SUCCESS-001")
                .build();
        SettableListenableFuture<SendResult<String, Object>> failureFuture =
                new SettableListenableFuture<>();
        failureFuture.setException(new RuntimeException("Temporary failure"));

        SettableListenableFuture<SendResult<String, Object>> successFuture =
                new SettableListenableFuture<>();
        SendResult<String, Object> sendResult = mock(SendResult.class);
        successFuture.set(sendResult);

        // First call fails, second succeeds
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(failureFuture)
                .thenReturn(successFuture);

        // Act
        producerService.sendOrder(order);

        // Assert
        verify(kafkaTemplate, times(2))
                .send(anyString(), eq("RETRY-SUCCESS-001"), eq(order));
        verify(dlqService, never()).sendToDLQ(any(), anyString());
    }
}
