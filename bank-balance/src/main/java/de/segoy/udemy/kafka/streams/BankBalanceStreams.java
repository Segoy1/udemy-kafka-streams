package de.segoy.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

public class BankBalanceStreams {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());


//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionSerdes.Transaction().getClass());
        ObjectMapper mapper = new ObjectMapper();
        Serde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class, mapper);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, transactionSerde.getClass());


        final StreamsBuilder builder = new StreamsBuilder();

        Transaction initialTransaction = Transaction.builder().build();

        final Consumed<String, Transaction> consumed = Consumed.with(Serdes.String(), transactionSerde);
        final KStream<String, Transaction> transactions = builder.stream("transactions", consumed);

        KTable<String, Transaction> bankBalance = transactions
                .selectKey((key, value) -> value.name)
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .aggregate(
                        () -> initialTransaction,
                        (key, newtransaction, balance) -> newTransaction(newtransaction, balance),
                        Materialized.<String, Transaction, KeyValueStore<Bytes, byte[]>>as("bank-balance-aggregate")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(transactionSerde)
                );

        bankBalance.toStream().to("bank-balance-once", Produced.with(Serdes.String(), transactionSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Transaction newTransaction(Transaction transaction, Transaction balance) {

        return Transaction.builder()
                .amount(balance.amount + transaction.amount)
                .name(transaction.getName())
                .time(transaction.getTime())
                .build();

    }

}
