package com.kafka.kimhabspringkafka.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Item;
import com.kafka.kimhabspringkafka.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {
    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    private final String TOPIC = "transactions";

    private final Random random = new Random();

//    @PostMapping
//    public String sendTransaction() throws Exception {
//
//        for (int i = 0; i < 50; i++) {
//
//            String transactionId = "txn-" + System.currentTimeMillis() + "-" + i;
//            double amount = 8000 + random.nextDouble() * (11000 - 8000);
//
//            Transaction txn = new Transaction(
//                    transactionId,
//                    "USER_" + i,
//                    amount, LocalDateTime.now().toString());
//
//           // String txnJson = mapper.writeValueAsString(txn);
//
//            kafkaTemplate.send(TOPIC, transactionId, txn);
//            log.info("👍 Sent Trx: [{}]", txn );
//        }
//
//        return "✅ Transaction sent to Kafka!";
//    }

    @PostMapping("/publish")
    public String publishTrx(){
        List<Transaction> transactionList = readTrxFromFile();

        for (Transaction trx: transactionList){
            kafkaTemplate.send(TOPIC, trx.transactionId(), trx);
            log.info("Send transactions : , {}", trx);
        }

        return "✅ Published " + transactionList.size() + " transactions to Kafka!";
    }

    @PostMapping("/trx-window-count")
    public String trxWindowCount() throws InterruptedException {
        log.info("🚀 Starting to publish random transactions...");

        // Define sample data
        List<String> users = Arrays.asList("User1", "User2", "User3");
        List<String> locations = Arrays.asList("India", "USA", "UK", "China");
        List<String> types = Arrays.asList("debit", "credit");

        // Loop to generate and send 15 random transactions
        for (int i = 0; i < 15; i++) {
            String user = users.get(random.nextInt(users.size())); // random users
            Transaction tx = new Transaction(
                    UUID.randomUUID().toString(), // unique uuid
                    user,
                    1000 + random.nextInt(9000), // random amount between 1000–9999
                    locations.get(random.nextInt(locations.size())),
                    types.get(random.nextInt(types.size())),
                    List.of( // random item list
                            new Item("I-" + random.nextInt(1000), "Product-" + random.nextInt(50), random.nextInt(5000), 1)
                    )
            );
            kafkaTemplate.send(TOPIC, user, tx); // send to kafka topic , user as key
            log.info("📤 Sent transaction for {}: {}", user, tx);

            // Small delay between messages so they spread across windows
            TimeUnit.SECONDS.sleep(5);
        }
        log.info("✅ Finished sending transactions!");
        return "Transactions published successfully!";
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
