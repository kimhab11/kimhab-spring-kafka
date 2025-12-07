package com.kafka.kimhabspringkafka.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaMetricsService  {
    private final Counter messagesProduced;
    private final Counter messagesConsumed;
    private final Counter messagesFailed;
    private final Timer producerLatency;
    private final Timer consumerProcessingTime;

    public KafkaMetricsService(MeterRegistry registry) {
        this.messagesProduced = Counter.builder("kafka.messages.produced")
                .description("Total messages produced")
                .tag("service", "kafka-producer")
                .tag("status", "success")
               // .tag("topic", topic)
                .register(registry);

        this.messagesConsumed = Counter.builder("kafka.messages.consumed")
                .description("Total messages consumed")
                .tag("service", "kafka-consumer")
                .register(registry);

        this.messagesFailed = Counter.builder("kafka.messages.failed")
                .description("Total messages failed")
                .tag("service", "kafka-consumer")
                .register(registry);

        this.producerLatency = Timer.builder("kafka.producer.latency")
                .description("Producer send latency")
                .publishPercentiles(0.5, 0.95, 0.99)
                .tag("status", "success")
                .publishPercentileHistogram(true)
                .register(registry);

        this.consumerProcessingTime = Timer.builder("kafka.consumer.processing.time")
                .description("Consumer processing time")
                .register(registry);
    }

    public void recordMessageProduced() {
        messagesProduced.increment();
    }

    public void recordMessageConsumed() {
        messagesConsumed.increment();
    }

    public void recordMessageFailed() {
        messagesFailed.increment();
    }

    public Timer.Sample startProducerTimer() {
        return Timer.start();
    }

    public void recordProducerLatency(Timer.Sample sample) {
        sample.stop(producerLatency);
    }

    public Timer.Sample startConsumerTimer() {
        return Timer.start();
    }

    public void recordConsumerProcessingTime(Timer.Sample sample) {
        sample.stop(consumerProcessingTime);
    }

}
