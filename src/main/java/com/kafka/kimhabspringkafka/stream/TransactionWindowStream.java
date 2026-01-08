package com.kafka.kimhabspringkafka.stream;

import com.kafka.kimhabspringkafka.model.Transaction;
import com.kafka.kimhabspringkafka.serde.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Slf4j
public class TransactionWindowStream {

    //source topic (transactions)
    //process (Windowing)10 > 3 -> fraud alert
    //write it back -> txn-fraud-alert

    @Bean
    public KStream<String, Transaction> windowedTrxStream(StreamsBuilder streamsBuilder){
        // create a stream from Kafka topic
        KStream<String, Transaction> stream = streamsBuilder.stream("transactions", Consumed.with(Serdes.String(),new TransactionSerde()));


        stream
                .groupBy(
                        ((key, value) -> value.userId()) // divide by user and repartition by userid
                        ,Grouped.with(Serdes.String(), new TransactionSerde()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))) // Apply a tumbling window of 10 seconds (no grace period)
                .count(Materialized.as("user-txn-count-window-store")) // Count number of transactions per user per window
                .toStream() // Convert KTable<Windowed<String>, Long> back to KStream
                .peek((windowedKey, count ) -> { // each record for logging and fraud detection
                    String user = windowedKey.key();
                    log.info("👥 User= {} , Count= {] , Window= [{}, {}]",
                            user,
                            count,
                            windowedKey.window().startTime(),
                            windowedKey.window().endTime());

                    // fraudulent ; if more than 3 transactions in 10s window
                    if (count > 3) {
                        log.warn("⚠️ FRAUD TRX ALERT: user: {}, mode: {}, trx within 10s", user, count);
                    }
                })
                // Send results to Kafka topic
                .to("user-txn-counts", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));

        return stream; // Return original stream for further processing if needed
    }

}
