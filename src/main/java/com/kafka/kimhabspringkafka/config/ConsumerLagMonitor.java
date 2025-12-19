package com.kafka.kimhabspringkafka.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerLagMonitor {
    private final KafkaListenerEndpointRegistry registry;
    private final MeterRegistry meterRegistry;

    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void monitorConsumerLag() {
        for (MessageListenerContainer container : registry.getAllListenerContainers()) {
            Consumer<?, ?> consumer = (Consumer<?, ?>) container
                    .getContainerProperties()
                    .getConsumerRebalanceListener();

            if (consumer != null) {
                Map<TopicPartition, Long> endOffsets =
                        consumer.endOffsets(consumer.assignment());

                for (TopicPartition partition : consumer.assignment()) {
                    OffsetAndMetadata committed =
                            consumer.committed(partition);

                    if (committed != null) {
                        long lag = endOffsets.get(partition) - committed.offset();

                        Gauge.builder("kafka.consumer.lag", () -> lag)
                                .tag("topic", partition.topic())
                                .tag("partition", String.valueOf(partition.partition()))
                                .description("Consumer lag for partition")
                                .register(meterRegistry);

                        log.debug("Consumer lag for {}: {}", partition, lag);
                    }
                }
            }
        }
    }
}
