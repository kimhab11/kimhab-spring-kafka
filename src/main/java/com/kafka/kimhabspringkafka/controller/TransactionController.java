package com.kafka.kimhabspringkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Random;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {
    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostMapping
    public String sendTransaction() throws Exception {

        for (int i = 0; i < 50; i++) {

            String transactionId = "txn-" + System.currentTimeMillis() + "-" + i;
            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);

            Transaction txn = new Transaction(
                    transactionId,
                    "USER_" + i,
                    amount, LocalDateTime.now().toString());

           // String txnJson = mapper.writeValueAsString(txn);

            kafkaTemplate.send("transactions", transactionId, txn);
            log.info("👍 Sent Trx: [{}]", txn );
        }

        return "✅ Transaction sent to Kafka!";
    }

}
