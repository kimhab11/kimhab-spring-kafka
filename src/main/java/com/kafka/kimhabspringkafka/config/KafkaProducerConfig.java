package com.kafka.kimhabspringkafka.config;

import com.kafka.kimhabspringkafka.service.DlqPublisher;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaProducerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private final MeterRegistry meterRegistry;

    public KafkaProducerConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic Configuration
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
      //  configProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
     //   configProps.put("schema.registry.url", schemaRegistryUrl);

        // ===== IDEMPOTENCE CONFIGURATION =====
        // Enables idempotent producer to prevent duplicate messages
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // ===== ACKNOWLEDGMENT CONFIGURATION =====
        // Wait for all in-sync replicas to acknowledge
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // ===== RETRY CONFIGURATION =====
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);  // Maximum number of retries (Integer.MAX_VALUE when idempotence is enabled)
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // Max in-flight requests (must be <= 5 for idempotence)

        // ===== TIMEOUT CONFIGURATION =====
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);  // Request 30s timeout
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 30s timeout (should be >= linger.ms + request.timeout.ms)

        // ===== BATCHING & PERFORMANCE =====
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);  // Batch size for better throughput
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 20); // Wait 20ms before sending batch message
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Faster Compression for network efficiency
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // Buffer memory
        log.info("{ Producer Config properties: {}", configProps.keySet());

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(configProps);
       // factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        factory.addListener(new MicrometerProducerListener<>(meterRegistry, Collections.singletonList(new ImmutableTag("component","kafka-producer"))));

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        log.info("{ Kafka Producer config");
        KafkaTemplate<String, Object> template = new  KafkaTemplate<>(producerFactory());
        template.setMicrometerEnabled(true);
        return template;

    }
}
