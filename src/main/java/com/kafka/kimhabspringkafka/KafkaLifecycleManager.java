package com.kafka.kimhabspringkafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLifecycleManager implements SmartLifecycle {
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private volatile boolean isRunning = false;

    @Override
    public void start() {
        log.info("Starting Kafka Listener Container");
        if (!kafkaListenerEndpointRegistry.isRunning()){
            kafkaListenerEndpointRegistry.start();
            isRunning = true;
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Kafka Listener Container gracefully");
        if (kafkaListenerEndpointRegistry.isRunning()){
            kafkaListenerEndpointRegistry.stop();
            isRunning = false;
            log.info("Kafka Listener Container stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase() {
        // Higher phase number = stops later (after most other beans)
        return Integer.MAX_VALUE - 1;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        log.info("Stopping with callback");
        this.stop();
        callback.run();
    }
}
