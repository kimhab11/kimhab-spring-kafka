package com.kafka.kimhabspringkafka.model;

//public record Transaction(
//        String transactionId,
//        String userId,
//        double amount,
//        String timestamp
//) {
//}

import java.util.List;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String location,
        String type,
        List<Item> items
) {
}