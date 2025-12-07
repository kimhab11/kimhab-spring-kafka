package com.kafka.kimhabspringkafka.controller;

import com.kafka.kimhabspringkafka.metric.KafkaMetricsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final KafkaMetricsService metricsService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("messagesSent", metricsService.getMessagesProduced().count());
        metrics.put("messagesFailed", metricsService.getMessagesFailed().count());
        metrics.put("messagesConsumed", metricsService.getMessagesConsumed().count());
       // metrics.put("messagesSentToDLQ

        return ResponseEntity.ok(metrics);
    }

 //   @PostMapping("/reset")
}