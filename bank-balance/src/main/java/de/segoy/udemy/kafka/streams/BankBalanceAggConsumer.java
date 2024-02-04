package de.segoy.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class BankBalanceAggConsumer {
    private static final Logger log = LoggerFactory.getLogger(BankBalanceAggConsumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello MotherFuckers, I am a producer");

        String groupId= "my-java-app";
        String topic = "bank-balance-once";

        // create Producer Properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        //create Consumer Config
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.setProperty(JsonDeserializer.VALUE_DEFAULT_TYPE, Transaction.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //"latest" for just reading new messages
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(properties);

        //Get a reference to the main Thread
        final Thread mainThread = Thread.currentThread();

        //add Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected Shutdown, exit by consumer.wakeup()...");
                consumer.wakeup();

                //join main thread to allow execution
                try {

                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });


        try {
            //subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for Data
            while (true) {


                ConsumerRecords<String, Transaction> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Transaction> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is starting to Shutdown");
        }catch (Exception e){
            log.error("Unexpected"+ e.getMessage());
        }finally {
            consumer.close();
            log.info("Consumer is shutdown gracefully");
        }

    }

}
