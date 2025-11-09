package com.kafka.kimhabspringkafka.controller;

import com.kafka.kimhabspringkafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
//@RequiredArgsConstructor
@Slf4j
public class CommandController {
    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final SimpMessageSendingOperations messagingTemplate;

    public CommandController(KafkaTemplate<String, Message> kafkaTemplate, SimpMessageSendingOperations messagingTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.messagingTemplate = messagingTemplate;
    }

    @PostMapping("/send")
    public void send(@RequestBody Message message) {
        kafkaTemplate.send("messaging", message);
        log.info("Message Request: {}", message);
        messagingTemplate.convertAndSend("/topic/public", message);
    }
}
