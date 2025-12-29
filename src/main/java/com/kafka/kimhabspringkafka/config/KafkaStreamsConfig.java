package com.kafka.kimhabspringkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public NewTopic createTransactionTopic() {
        log.info("😍 transactions CREATE");
        return new NewTopic("transactions",
                3, (short) 1);
    }

    @Bean
    public NewTopic createFraudAlertTopic() {
        log.info("😍 fraud-alerts CREATE");
        return new NewTopic("fraud-alerts",
                3, (short) 1);
    }

}
