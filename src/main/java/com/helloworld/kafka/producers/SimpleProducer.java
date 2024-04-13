package com.helloworld.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Crear el productor
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Enviar un mensaje
        String topic = "test-topic";
        String key = "sample-key";
        String value = "sample-value2";
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, key, value);
        producer.send(kafkaRecord);

        // Cerrar el productor
        producer.close();
    }
}
