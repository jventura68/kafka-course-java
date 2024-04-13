package com.helloworld.kafka.ejercicios;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AllTopicsConsumer {

    public static void main (final String[] args) throws Exception {
        String configFile = "./config/default.properties";

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Define grupo y offset
    	final String consumerGroup = MethodHandles.lookup().lookupClass()+"Group";
        props.put("group.id", "migrupo");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pattern pattern = Pattern.compile(".*");
        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(pattern);  
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(
                            String.format("Consumed event from topic:%s key = %-10s value = %s", record.topic(), key, value));
                }
            }
        }
    }
    
}
