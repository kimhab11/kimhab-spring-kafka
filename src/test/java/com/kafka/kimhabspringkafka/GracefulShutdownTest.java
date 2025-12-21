package com.kafka.kimhabspringkafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.immediate-stop=false",
        "spring.lifecycle.timeout-per-shutdown-phase=30s"
})
public class GracefulShutdownTest {

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Test
    public void testGracefulShutdown() throws InterruptedException {
        // Verify listener is running
        assert kafkaListenerEndpointRegistry.isRunning();

        // Stop gracefully
        kafkaListenerEndpointRegistry.stop();

        // Verify listener stopped
        Thread.sleep(2000); // Wait for graceful shutdown
        assert !(kafkaListenerEndpointRegistry.isRunning());
    }
}
