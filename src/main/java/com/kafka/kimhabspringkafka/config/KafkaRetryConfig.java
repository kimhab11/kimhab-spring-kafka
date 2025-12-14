package com.kafka.kimhabspringkafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;

@Configuration
public class KafkaRetryConfig {

    /**
     * Programmatic RetryTopicConfiguration (optional).
     * If you prefer annotation-only, you can omit this bean.
     */
    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(KafkaTemplate<String, Object> kafkaTemplate) {
        return RetryTopicConfigurationBuilder
                .newInstance()
                .exponentialBackoff(2000L, 2.0, 10000L) // initialDelay, multiplier, maxDelay
                .maxAttempts(4) // total attempts (initial + retries)
                .dltSuffix(".DLT") // DLT suffix
                .create(kafkaTemplate);
    }
}
