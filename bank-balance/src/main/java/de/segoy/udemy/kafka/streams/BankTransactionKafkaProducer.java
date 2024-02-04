package de.segoy.udemy.kafka.streams;

import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.time.Instant;
import java.util.*;


public class BankTransactionKafkaProducer {
    public static void main(String[] args) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(config);

        Set<String> customers = new HashSet<>(Set.of("John", "Harry", "Lenn", "Lisa", "Abella", "Segoy"));

        while (true) {

            for (String customer : customers) {
                Transaction transaction =
                        Transaction.builder().name(customer).amount(randomAmount()).time(Instant.now().toEpochMilli()).build();
                ProducerRecord<String, Transaction> record = new ProducerRecord<>("transactions",transaction);
                producer.send(record);
                System.out.println(record.value().name);
            }
            try {
            Thread.sleep(50L);
            }catch (Exception e){
                System.out.println("SomethingWong");
            }

        }
    }
        private static int randomAmount(){
            return (int) ((Math.random() * (1000)) - 600);
        }
}
