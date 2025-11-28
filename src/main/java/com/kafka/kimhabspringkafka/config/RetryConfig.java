package com.kafka.kimhabspringkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
/**
 * Enable Spring Retry mechanism
 * This allows @Retryable annotations to work
 */

@EnableRetry
public class RetryConfig {
    // No additional configuration needed
    // @EnableRetry activates the retry functionalit
}
