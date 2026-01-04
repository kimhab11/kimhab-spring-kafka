package com.kafka.kimhabspringkafka.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Item;
import com.kafka.kimhabspringkafka.model.Transaction;
import com.kafka.kimhabspringkafka.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@EnableKafkaStreams
public class FraudDetectionStream {

    //create bean
    //-> read the topic
    //-> process filter
    //-> write to dest

//    @Bean
//    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {
//
//       // JsonSerde<Transaction> transactionJsonSerde = new JsonSerde<>(Transaction.class);
//
//        KStream<String, Transaction> transactionKStream =
//                builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

//        transactionKStream
//                .filter((key, value) -> value.amount() > 10000)
//                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT - transactionId={}", value))
//                .to("fraud-alerts", Produced.with(Serdes.String(),  new TransactionSerde()));
//
//        return transactionKStream;
//    }

        // == style 2 ====

//        // Step 1: Read messages from the input topic.
//        KStream<String, String> transactionsStream = builder.stream("transactions");
//
//        // Step 2: Process the stream to detect fraudulent transactions.
//        KStream<String, String> fruadTrxStream = transactionsStream.
//                filter(((key, value) -> isSuspicious(value)))
//                .peek((key, value) -> {
//                    log.warn("⚠️ FRAUD ALERT - transactionId={} , value={}", key, value);
//                });
//        // Step 3: write detected fraudulent transactions to an output topic.
//        fruadTrxStream.to("fraud-alerts");
//        return transactionsStream;
//
//    }



 //   ==== 1. style1 ======

//    public void fraudDetectStreamFunctionalStyle(StreamsBuilder builder) {
//
//         builder
//                .stream("transactions")
//                .filter((key, value) -> isSuspicious((String) value))
//                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT - transactionId={}, value={}", key, value))
//                .to("fraud-alerts");
//
//
//    }

    private boolean isSuspicious(String value) {

        try {
            Transaction transaction = new ObjectMapper().readValue(value, Transaction.class);
            return transaction.amount() > 10000; // simple fraud rule
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }


    // test with fuctions
    //    groupBy(),
    //    aggregate(),
    //    count()
    @Bean
    public KStream<String, Transaction> fraudDetectStreamWithMethod(StreamsBuilder streamsBuilder){
        KStream<String, Transaction> stream =
                streamsBuilder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        // filter
//        stream
//                .filter((key, value) -> value.amount() > 25000)
//                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT for {}", value));

        // filterNot
//        stream
//                .filter((key, value) -> value.amount() < 10000)
//                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT for {}", value));

        // map and mapValue
//        stream
//                .map((key, value) -> KeyValue.pair(value.userId(), "User spent amount: " +value.amount()))
//                .peek((key, value) -> log.info("👤 map() User Transaction Summary: Key: {}, Value: {}", key, value));
//        stream.
//                mapValues(value -> "Transaction of "+value.amount()+ " by user: "+ value.userId())
//                .peek((key, value) -> log.info("👤 mapValues() User Transaction Summary Value Only: Key: {}, Value: {}", key, value));

        // flatMap and flatMapValue
//        stream
//                .flatMap((key, value) -> {
//                    List<KeyValue<String, Item>> result = new ArrayList<>();
//                    for (Item item: value.items()){
//                        result.add(KeyValue.pair(value.transactionId(), item));
//                    }
//                    return result;
//                })
//                .peek((key, item) -> log.info("flatMap, 😊 Item Purchased: Transaction ID: {}, Item: {}", key, item));
//        stream
//                .flatMapValues(Transaction::items)
//                .peek((key, value) -> log.info("flatMapValues - 🙌 Item Purchased Value Only: Transaction ID: {}, Item: {}", key, value));

         // branch()
//        KStream<String, Transaction>[] branch = stream
//                .branch(
//                        (key, value) -> value.type().equalsIgnoreCase("debit"),
//                        (key, value) -> value.type().equalsIgnoreCase("credit")
//                );
//        branch[0].peek((key, value) -> log.info(" 💳 Debit Transaction: Key: {}, Transaction: {}", key, value));
//        branch[1].peek((key, value) -> log.info(" 💳 Credit Transaction: Key: {}, Transaction: {}", key, value));

        // groupBy with stateful
//        stream
//                .groupBy((key, value) -> value.location())
//                .count()
//                .toStream()
//                .peek((location, count) -> log.info("📍 Location: {} has trx Count: {}", location, count ));

        // groupBy with stateful with custom state
//        stream
//                .groupBy((key, value) -> value.userId())
//                .count(Materialized.as("user-trx-count-store"))
//                .toStream()
//                .peek((userId, count) ->  log.info("👥 User {} made {} transactions", userId, count));

        // aggregate
        stream
                .groupBy((key, value) -> value.type())
                .aggregate(
                        () -> 0.0,
                        (typ, trx, currentSum) -> currentSum + trx.amount(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                ).toStream()
                .peek((type, total) -> {
                    log.info("CardType: {} | 💰 Running Total Amount: {}", type, total);
                });

        return stream;

    }
}
