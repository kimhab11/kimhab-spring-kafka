package com.kafka.kimhabspringkafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DLQMessage {
    private OrderEvent originalMessage;
    private String errorReason;
    private LocalDateTime failedAt;
    private String originalTopic;
    private int retryCount;
    private String stackTrace;
}
