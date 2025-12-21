# Kafka Producer with Idempotence, acks=all, @Retryable, and DLQ

Complete Spring Boot application demonstrating Kafka best practices for reliable message delivery.

## Features

✅ **Idempotent Producer** - Prevents duplicate messages   
✅ **Compression Reduce** - network bandwidth (Snappy compression)   
✅ **acks=all** - Waits for all in-sync replicas  
✅ **@Retryable** - Automatic retry with exponential backoff  
✅ **Dead Letter Queue (DLQ)** - Failed message handling  
✅ **Manual Offset Commit** - Consumer control  
✅ **Health Checks** - Kafka cluster monitoring     
✅ **Performance & Observability** Use Micrometer for metrics (e.g., consumer lag, send latency).  
✅ **Concurrency** - Multiple consumer threads for scalability  
✅ **Graceful Shutdown** - Clean application exit  
✅ **Docker Compose** - Easy local development

## Run Application
```bash
mvn clean install
mvn spring-boot:run
```






