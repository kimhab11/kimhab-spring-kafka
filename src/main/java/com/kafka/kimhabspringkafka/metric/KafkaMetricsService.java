package com.kafka.kimhabspringkafka.metric;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class KafkaMetricsService {

    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong messagesRetried = new AtomicLong(0);
    private final AtomicLong messagesSentToDLQ = new AtomicLong(0);

    public void incrementMessagesSent() {
        messagesSent.incrementAndGet();
        log.debug("Messages sent: {}", messagesSent.get());
    }

    public void incrementMessagesFailed() {
        messagesFailed.incrementAndGet();
        log.warn("Messages failed: {}", messagesFailed.get());
    }

    public void incrementMessagesRetried() {
        messagesRetried.incrementAndGet();
        log.info("Messages retried: {}", messagesRetried.get());
    }

    public void incrementMessagesSentToDLQ() {
        messagesSentToDLQ.incrementAndGet();
        log.error("Messages sent to DLQ: {}", messagesSentToDLQ.get());
    }

    public long getMessagesSent() {
        return messagesSent.get();
    }

    public long getMessagesFailed() {
        return messagesFailed.get();
    }

    public long getMessagesRetried() {
        return messagesRetried.get();
    }

    public long getMessagesSentToDLQ() {
        return messagesSentToDLQ.get();
    }

    public void resetMetrics() {
        messagesSent.set(0);
        messagesFailed.set(0);
        messagesRetried.set(0);
        messagesSentToDLQ.set(0);
        log.info("Metrics reset");
    }
}
