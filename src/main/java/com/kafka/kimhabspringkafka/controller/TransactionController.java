package com.kafka.kimhabspringkafka.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {
    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

//    @PostMapping
//    public String sendTransaction() throws Exception {
//
//        for (int i = 0; i < 50; i++) {
//
//            String transactionId = "txn-" + System.currentTimeMillis() + "-" + i;
//            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
//
//            Transaction txn = new Transaction(
//                    transactionId,
//                    "USER_" + i,
//                    amount, LocalDateTime.now().toString());
//
//           // String txnJson = mapper.writeValueAsString(txn);
//
//            kafkaTemplate.send("transactions", transactionId, txn);
//            log.info("👍 Sent Trx: [{}]", txn );
//        }
//
//        return "✅ Transaction sent to Kafka!";
//    }


    @PostMapping("/publish")
    public String publishTrx(){
        List<Transaction> transactionList = readTrxFromFile();

        for (Transaction trx: transactionList){
            kafkaTemplate.send("transactions", trx.transactionId(), trx);
            log.info("Send transactions : , {}", trx);
        }

        return "✅ Published " + transactionList.size() + " transactions to Kafka!";
    }

    private List<Transaction> readTrxFromFile(){
        // open file resource from location
        try (InputStream is = getClass().getResourceAsStream("/trx.json")) {
            return mapper.readValue(is, new TypeReference<List<Transaction>>() {
            });
        } catch (IOException e) {
            // wrap any parsing errors and rethrow as a runtime exception
            throw new RuntimeException(e);
        }
    }

}
