import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumerExample {
    public static void main(String[] args) {
        // Configuración del consumidor
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8085"); // URL del Schema Registry
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Crear consumidor
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        final String topic = "test-topic-avro";

        // Suscribirse al topic
        consumer.subscribe(Collections.singletonList(topic));

        // Procesar mensajes
        try {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.printf("Consumed record with key %s and value %s, and updated stock count to %s%n",
                            record.key(), record.value().toString());
                });
            }
        } finally {
            consumer.close();
        }
    }
}
