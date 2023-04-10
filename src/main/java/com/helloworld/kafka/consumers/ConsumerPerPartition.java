package com.helloworld.kafka.consumers;



import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ConsumerPerPartition {
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC = "test-topic";
    private static final String GROUP_ID = "ConsumerPerPartition-group";
    private static final int NUM_CONSUMERS = 3;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
        List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

        try {
            for (int i = 0; i < NUM_CONSUMERS; i++) {
            	final int numConsumer = i;
                KafkaConsumer<String, String> consumer = createConsumer();
                consumers.add(consumer);
                executor.submit(() -> runConsumer(consumer, numConsumer));
            }

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("enable.auto.commit", false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return new KafkaConsumer<>(props);
    }

    private static void runConsumer(KafkaConsumer<String, String> consumer, int numConsumer) {
        try {
        	
            consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Commit any offsets before revoking the partitions
                    consumer.commitSync();
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Do nothing
                }
            });

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                processRecords(records, consumer, numConsumer);
            }
        } catch (WakeupException e) {
            // Ignored for shutdown
        } finally {
            consumer.close();
        }
    }

    private static void processRecords(ConsumerRecords<String, String> records, KafkaConsumer<String, String> consumer, int numConsumer) {
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("Consumer = %s, offset = %d, partition = %d, key = %s, value = %s%n", 
            		//consumer.groupMetadata().memberId()
            		numConsumer,  record.offset(), record.partition(), record.key(), record.value());
        }

        //System.out.println("Se ha finalizado el procesamiento del pool");
    }
}
