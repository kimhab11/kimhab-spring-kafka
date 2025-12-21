package com.kafka.kimhabspringkafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
@Slf4j
public class KafkaShutdownHook implements ApplicationListener<ContextClosedEvent> {
    private final KafkaListenerEndpointRegistry registry;

    public KafkaShutdownHook(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        // Stop all Kafka listeners gracefully
        registry.getAllListenerContainers().forEach(container -> {
            log.warn("Stopping kafka listener container: {}", container);
            container.stop(() -> {
                log.warn("Kafka listener container stopped gracefully: {}" ,container);
            });
        });
    }

    @PreDestroy
    public void onShutdown() {
        try {
            // Stop kafka listeners - they will finish processing current batch
            registry.stop();
            log.info("Kafka listeners stopped gracefully");

        } catch (Exception e) {
            log.error("Error during graceful shutdown", e);
        }
        log.info("========== KAFKA SHUTDOWN COMPLETED ==========");
    }
}
