package com.kafka.kimhabspringkafka.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
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
    public KStream<String, String> fraudDetectStream(StreamsBuilder builder) {

        // Step 1: Read messages from the input topic.
        KStream<String, String> transactionsStream = builder.stream("transactions");

        // Step 2: Process the stream to detect fraudulent transactions.
        KStream<String, String> fruadTrxStream = transactionsStream.
                filter(((key, value) -> isSuspicious(value)))
                .peek((key, value) ->  {
                    log.warn("⚠️ FRAUD ALERT - transactionId={} , value={}", key, value);
                });
        // Step 3: write detected fraudulent transactions to an output topic.
        fruadTrxStream.to("fraud-alerts");
        return transactionsStream;

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


    }

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
