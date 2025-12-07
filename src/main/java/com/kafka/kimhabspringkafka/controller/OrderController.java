package com.kafka.kimhabspringkafka.controller;

import com.kafka.kimhabspringkafka.dto.OrderEvent;
import com.kafka.kimhabspringkafka.dto.OrderRequest;
import com.kafka.kimhabspringkafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {
    private final KafkaProducerService kafkaProducerService;
    /**
     * Create order - async with retry and DLQ
     */
    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderRequest request) {
        try {
            OrderEvent order = OrderEvent.builder()
                    .orderId(UUID.randomUUID().toString())
                    .customerId(request.getCustomerId())
                    .amount(request.getAmount())
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendOrder(order);

            return ResponseEntity.accepted()
                    .body("Order submitted successfully: " + order.getOrderId());

        } catch (Exception e) {
            log.error("Failed to submit order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to submit order: " + e.getMessage());
        }
    }

    /**
     * Create order - synchronous
     */
    @PostMapping("/sync")
    public ResponseEntity<String> createOrderSync(@RequestBody OrderRequest request) {
        try {
            OrderEvent order = OrderEvent.builder()
                    .orderId(UUID.randomUUID().toString())
                    .customerId(request.getCustomerId())
                    .amount(request.getAmount())
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            kafkaProducerService.sendOrderSync(order);

            return ResponseEntity.ok("Order created successfully: " + order.getOrderId());

        } catch (Exception e) {
            log.error("Failed to create order synchronously", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to create order: " + e.getMessage());
        }
    }


    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Service is running");
    }

}
