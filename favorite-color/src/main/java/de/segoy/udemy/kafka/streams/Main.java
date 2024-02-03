package de.segoy.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        KafkaStreams streams = new KafkaStreams(createTopology(), config);
        streams.start();


        //Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }


    private static Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> favColorInput = builder.stream("fav-color-input");

        KTable<String, String> favColorTable = favColorInput.mapValues(value -> value.toLowerCase())
                .filter((key, value)-> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0])
                .mapValues(value-> value.split(",")[1])
                .filter((key,value)-> value.equals("red")||value.equals("blue")||value.equals("green"))
                .toTable();

        favColorTable.toStream().to("fav-color-intermediate", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String,String> favColor = builder.table("fav-color-intermediate");

        KTable<String, Long> output = favColor
                .groupBy((user, color)-> new KeyValue<>(color,color))
                .count(Materialized.as("Counts"));

        output.toStream().to("fav-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
