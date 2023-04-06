package com.helloworld.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SyncProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Crear el productor
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Enviar un mensaje
        String topic = "test-topic";
        String key = "synchronous";
        String value = "Write sync message";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata metadata = future.get(); // Bloquea la ejecución hasta que se complete el envío
            System.out.printf("Mensaje enviado al topic: %s, partición: %d, offset: %d%n", metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error al enviar el mensaje: " + e.getMessage());
        }
        // Cerrar el productor
        producer.close();
    }

}
