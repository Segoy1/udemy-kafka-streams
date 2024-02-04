package de.segoy.udemy.kafka.streams;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerdes {

    public static Serde<Transaction> Transaction() {
        JsonSerializer<Transaction> serializer = new JsonSerializer<>();
        JsonDeserializer<Transaction> deserializer = new JsonDeserializer<>(Transaction.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}

