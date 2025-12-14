package com.kafka.kimhabspringkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DlqPublisher {
    private String dlqTopic = "orders-topic-dlq";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DlqPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishToDlq(ConsumerRecord<?, ?> record, Exception exception) {
        String key = record.key() != null ? record.key().toString() : null;
        String value = record.value() != null ? record.value().toString() : null;

        // Add error metadata as headers
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("dlq-original-topic",
                record.topic().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq-original-partition",
                String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq-original-offset",
                String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq-exception-message",
                exception.getMessage().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq-exception-class",
                exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq-timestamp",
                Instant.now().toString().getBytes(StandardCharsets.UTF_8)));

        // Copy original headers
        if (record.headers() != null) {
            record.headers().forEach(headers::add);
        }

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                dlqTopic, null, key, value, headers
        );

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(dlqRecord);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Failed to send message to DLQ: {}", ex.getMessage(), ex);
            }
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Message sent to DLQ: topic={}, partition={}, offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

}
