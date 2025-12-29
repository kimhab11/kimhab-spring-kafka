package com.kafka.kimhabspringkafka.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Transaction;
import com.kafka.kimhabspringkafka.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@EnableKafkaStreams
public class FraudDetectionStream {

    //create bean
    //-> read the topic
    //-> process filter
    //-> write to dest

    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

       // JsonSerde<Transaction> transactionJsonSerde = new JsonSerde<>(Transaction.class);

        KStream<String, Transaction> transactionKStream =
                builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));
        System.out.println("Consumed transactions");

        transactionKStream
                .filter((key, value) -> value.amount() > 10000)
                .peek((key, value) -> log.warn("⚠️ FRAUD ALERT - transactionId={}", value))
                .to("fraud-alerts", Produced.with(Serdes.String(),  new TransactionSerde()));

        return transactionKStream;
    }

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
}
