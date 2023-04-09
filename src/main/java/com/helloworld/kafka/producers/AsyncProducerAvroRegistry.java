package com.helloworld.kafka.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducerAvroRegistry {

    private static final Logger log = LoggerFactory.getLogger(AsyncProducerAvro.class);

    public static void main(final String[] args) throws IOException, RestClientException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        //propiedad para la localización del registry
        props.put("schema.registry.url", "http://localhost:8085");

        final String topic = "test-topic-avro";

        final Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props);

        // Recuperación de esquemas desde el registry
        String keySubject = "test-key"; // nombre dado al esquema en el registry
        String valueSubject = "test-value"; 

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8085", 100);
        SchemaMetadata keySchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(keySubject);
        SchemaMetadata valueSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(valueSubject);

        Schema keySchema = new Schema.Parser().parse(keySchemaMetadata.getSchema());
        Schema valueSchema = new Schema.Parser().parse(valueSchemaMetadata.getSchema());


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
}
