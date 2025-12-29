package com.kafka.kimhabspringkafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kimhabspringkafka.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TransactionDeserializer implements Deserializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Transaction deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, Transaction.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
