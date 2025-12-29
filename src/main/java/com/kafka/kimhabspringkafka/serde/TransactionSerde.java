package com.kafka.kimhabspringkafka.serde;

import com.kafka.kimhabspringkafka.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
    public TransactionSerde(Serializer<Transaction> serializer, Deserializer<Transaction> deserializer) {
        super(serializer, deserializer);
    }

    public TransactionSerde() {
        super(new TransactionSerializer(), new TransactionDeserializer());
    }
}
