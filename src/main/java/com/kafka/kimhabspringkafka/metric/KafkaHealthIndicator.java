package com.kafka.kimhabspringkafka.metric;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;

    @Override
    public Health health() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterOptions options = new DescribeClusterOptions()
                    .timeoutMs(5000);
            DescribeClusterResult clusterResult = adminClient.describeCluster();

            adminClient.describeCluster(options)
                    .clusterId()
                    .get(5, TimeUnit.SECONDS);

            String clusterId = clusterResult.clusterId().get(5, TimeUnit.SECONDS);
            int nodeCount = clusterResult.nodes().get(5, TimeUnit.SECONDS).size();

            return Health.up()
                    .withDetail("kafka", "Connected")
                    .withDetail("clusterId", clusterId)
                    .withDetail("brokerCount", nodeCount)
                    .build();

        } catch (Exception e) {
            log.error("Kafka health check failed", e);
            return Health.down(e)
                    .withDetail("kafka", "Unavailable")
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }

    // Get partition count for a topic
    public int getPartitionCount(String topic) throws ExecutionException, InterruptedException {
        try (AdminClient admin =  AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return admin.describeTopics(java.util.Collections.singleton(topic))
                    .allTopicNames()
                    .get()
                    .get(topic)
                    .partitions()
                    .size();
        }
    }
}
