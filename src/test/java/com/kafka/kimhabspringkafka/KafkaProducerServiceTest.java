package com.kafka.kimhabspringkafka;

import com.kafka.kimhabspringkafka.dto.OrderEvent;
import com.kafka.kimhabspringkafka.service.DeadLetterQueueService;
import com.kafka.kimhabspringkafka.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Slf4j

class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private DeadLetterQueueService dlqService;

    @InjectMocks
    private KafkaProducerService producerService;
    @InjectMocks
    private OrderEvent testOrder;

    @BeforeEach
    void setUp() {
        testOrder = OrderEvent.builder()
                .orderId("ORDER-123")
                .customerId("CUST-001")
                .amount(new BigDecimal("100.00"))
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();
    }

    // ========== SUCCESS SCENARIOS ==========

    @Test
    void testOrder_Success(){
        // Arrange
        SettableListenableFuture<SendResult<String, Object>> future = new SettableListenableFuture<>();
        SendResult<String, Object> sendResult = mock(SendResult.class);

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Simulate successful send
        future.set(sendResult);

        // Act
        producerService.sendOrder(testOrder);

        // Assert
        verify(kafkaTemplate, times(1)).send(anyString(), eq("TEST-001"), eq(testOrder));
        verify(dlqService, never()).sendToDLQ(any(), anyString());

    }

    // ========== FAILURE SCENARIOS ==========

    @Test
    void testOrder_KafkaException_TriggersRetry(){
        // Arrange
        SettableListenableFuture<SendResult<String, Object>> future = new SettableListenableFuture<>();

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Simulate Kafka failure
        future.setException(new RuntimeException("Kafka broker unavailable"));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            producerService.sendOrder(testOrder);

        });

        assertTrue(exception.getMessage().contains("Error sending order to Kafka"));
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), eq("ORDER-123"), eq(testOrder));
    }

    @Test
    void testOrder_ExecutionException(){
        // Arrange
        SettableListenableFuture<SendResult<String, Object>> future = new SettableListenableFuture<>();

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Simulate execution exception
        future.setException(new ExecutionException(new RuntimeException("Connection timeout")));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            producerService.sendOrder(testOrder);
        });

        verify(kafkaTemplate).send(anyString(), eq("ORDER-123"), eq(testOrder));
    }

    @Test
    void testOrder_TimeoutException() throws ExecutionException, InterruptedException {
        // Arrange
        ListenableFuture<SendResult<String, Object>> future = mock(ListenableFuture.class);

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Simulate timeout on future.get()
        when(future.get()).thenThrow(new TimeoutException("Request timeout"));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            producerService.sendOrder(testOrder);
        });

        verify(kafkaTemplate).send(anyString(), eq("ORDER-123"), eq(testOrder));

    }

    @Test
    void testOrder_InterruptedException() throws ExecutionException, InterruptedException {
        // Arrange
        ListenableFuture<SendResult<String, Object>> future = mock(ListenableFuture.class);

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        when(future.get()).thenThrow(new InterruptedException("Thread interrupted"));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            producerService.sendOrder(testOrder);
        });

        verify(kafkaTemplate).send(anyString(), eq("ORDER-123"), eq(testOrder));
    }

    // ========== RECOVERY/DLQ SCENARIOS ==========

    @Test
    void testRecoverFromSendFailure_SendsToDLQ() {
        // Arrange
        Exception testException = new RuntimeException("All retries exhausted");
        doNothing().when(dlqService).sendToDLQ(any(), anyString());

        // Act
        producerService.recoverFromSendFailure(testException, testOrder);

        // Assert
        verify(dlqService, times(1)).sendToDLQ(
                eq(testOrder),
                eq("All retries exhausted")
        );
    }

    @Test
    void testRecoverFromSendFailure_DLQFails() {
        // Arrange
        Exception originalException = new RuntimeException("Kafka send failed");
        doThrow(new RuntimeException("DLQ unavailable"))
                .when(dlqService).sendToDLQ(any(), anyString());

        // Act - should not throw, just log
        assertDoesNotThrow(() -> {
            producerService.recoverFromSendFailure(originalException, testOrder);
        });

        // Assert
        verify(dlqService, times(1)).sendToDLQ(eq(testOrder), anyString());
    }

    // ========== CALLBACK FAILURE SCENARIOS ==========

    @Test
    void testOrder_CallbackFailure() throws Exception {
        // Arrange
        SettableListenableFuture<SendResult<String, Object>> future = new SettableListenableFuture<>();

        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(future);

        // Simulate callback failure after future.get() completes
        SendResult<String, Object> sendResult = mock(SendResult.class);
        future.set(sendResult);

        // Act
        producerService.sendOrder(testOrder);

        // Assert - successful send
        verify(kafkaTemplate, times(1)).send(anyString(), eq("ORDER-123"), eq(testOrder));
    }
}