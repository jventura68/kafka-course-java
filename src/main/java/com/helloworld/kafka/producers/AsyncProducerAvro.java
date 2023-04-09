package com.helloworld.kafka.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducerAvro {

    private static final Logger log = LoggerFactory.getLogger(AsyncProducerAvro.class);

    public static void main(final String[] args) throws IOException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8085");

        final String topic = "test-topic-avro";

        final Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props);
        String keySchemaString = """
       		{
			  "namespace": "test",
			  "type": "record",
			  "name": "key",
			  "fields": [
			    {
			      "name": "key",
			      "type": "string"
			    }
			  ]
			}
   		""";
        String valueSchemaString = readFileFromResources("test.value.avsc");
        Schema keySchema = new Schema.Parser().parse(keySchemaString);
        Schema valueSchema = new Schema.Parser().parse(valueSchemaString);

        // Crear y enviar registros
        final Random rnd = new Random();
        final Long numMessages = 10L;
        for (Long i = 0L; i < numMessages; i++) {
            GenericRecord keyRecord = new GenericData.Record(keySchema);
            keyRecord.put("key", "key_" + i);

            GenericRecord valueRecord = new GenericData.Record(valueSchema);
            valueRecord.put("name", "name_" + i);
            valueRecord.put("city", "city_" + i);
            valueRecord.put("phone", rnd.nextInt(1000000000));
            valueRecord.put("age", rnd.nextInt(100));

            producer.send(
            		new ProducerRecord<GenericRecord, GenericRecord>(topic, keyRecord, valueRecord),
                    (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.printf("Produced event to topic %s: key= %s, value = %s, partition=%d%n",
                                    metadata.topic(), keyRecord.toString(), valueRecord.toString(), metadata.partition());
                        }
                    }
            );
        }

        System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        producer.flush();
        producer.close();
    }
    
    public static String readFileFromResources(String fileName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException("Error al leer el archivo " + fileName + " desde src/main/resources", e);
        }
    }
}
