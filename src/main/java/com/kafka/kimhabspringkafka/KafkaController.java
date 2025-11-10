package com.kafka.kimhabspringkafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendMessage(
            @RequestParam String topic,
            @RequestParam String message) {

        producerService.sendMessage(topic, message);

        Map<String, String> response = new HashMap<>();
        response.put("status", "Message sent");
        response.put("topic", topic);
        response.put("message", message);

        return ResponseEntity.ok(response);
    }

    @PostMapping("/send-user-event")
    public ResponseEntity<Map<String, String>> sendUserEvent(
            @RequestParam String userId,
            @RequestParam String action) {

        UserEvent event = new UserEvent(userId, action, System.currentTimeMillis());
        producerService.sendUserEvent(event);

        Map<String, String> response = new HashMap<>();
        response.put("status", "User event sent");
        response.put("userId", userId);
        response.put("action", action);

        return ResponseEntity.ok(response);
    }

    @PostMapping("/send-async")
    public ResponseEntity<Map<String, String>> sendMessageAsync(
            @RequestParam String topic,
            @RequestParam String message) {

        producerService.sendMessageAsync(topic, message);

        Map<String, String> response = new HashMap<>();
        response.put("status", "Message sent asynchronously");

        return ResponseEntity.ok(response);
    }
}

