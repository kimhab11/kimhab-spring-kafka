package com.kafka.kimhabspringkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
public class ShutdownConfig {
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    // Custom business logic shutdown
    @PreDestroy
    public void onShutdown() {
        System.out.println("Starting graceful shutdown of custom resources...");

        // Shutdown executor service gracefully
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.warn("Custom resources shutdown complete.");
    }
}
