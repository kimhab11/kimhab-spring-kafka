package com.kafka.kimhabspringkafka.config;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.validate.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.FixedBackOff;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@Configuration
@Slf4j
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;


    private final MeterRegistry meterRegistry;

    public KafkaConsumerConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerConfig(){
        Map<String, Object> consProps = new HashMap<>();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consProps.put(GROUP_ID_CONFIG, groupId);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        //        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
//        consProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
//        consProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        consProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual acknowledgment
        consProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");  // Read only committed messages
        consProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        consProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        consProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        log.info("{ Kafka Listener Config, GROUP_ID_CONFIG: {}", consProps.get(GROUP_ID_CONFIG));
        consProps.put("schema.registry.url", schemaRegistryUrl);

        DefaultKafkaConsumerFactory<String, Object> cf = new DefaultKafkaConsumerFactory<>(consProps);
        cf.addListener(new MicrometerConsumerListener<>(meterRegistry, Collections.singletonList(new ImmutableTag("component","kafka-consumer"))));
        return cf;

    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(KafkaTemplate<String, Object> kafkaTemplate, ThreadPoolTaskExecutor kafkaConsumerTaskExecutor) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerConfig());
        factory.setConcurrency(3);  // Set number of concurrent threads (should not exceed partition count)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);// Use manual ack so we can acknowledge after async processing completes
        factory.getContainerProperties().setPollTimeout(3000); // Poll timeout tuning
        factory.getContainerProperties().setMicrometerEnabled(true);  // enable per-listener observation and timers
        factory.setBatchListener(true); // // Batch listener (optional - for processing multiple records at once)

        // Configure error handler with DLT
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, exception) -> {
                            // Send to dead-letter topic with suffix
                            return new TopicPartition(
                                    record.topic() + ".DLT",
                                    record.partition());
                        }),
                new FixedBackOff(1000L, 3L) // 3 retries with 1s delay(1 second backoff)
        );

        // Don't retry for specific exceptions
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                ValidationException.class,
                DeserializationException.class
        );
        // Log failures
        errorHandler.setLogLevel(KafkaException.Level.WARN);

        factory.setCommonErrorHandler(errorHandler);
        log.info("{ Concurrent Kafka Listener config");
        return factory;
    }

    // Custom Thread Pool Configuration
    @Bean
    public ThreadPoolTaskExecutor kafkaConsumerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(15);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("kafka-consumer-");
        executor.setAllowCoreThreadTimeOut(true);
        executor.initialize();
        return executor;
    }

    // Executor for async processing handoff
    @Bean(destroyMethod = "shutdown")
    public Executor kafkaProcessingExecutor(){
        return Executors.newFixedThreadPool(8);
    }
}
