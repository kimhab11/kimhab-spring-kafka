package com.kafka.kimhabspringkafka.model;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {
}