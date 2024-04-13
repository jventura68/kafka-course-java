package com.helloworld.kafka.registry;

import java.io.IOException;
import java.util.Collection;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class ListSchemas {

    public static void main(String[] args) throws IOException {
        String schemaRegistryUrl = "http://localhost:8085";
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        try {
            Collection<String> subjects = schemaRegistryClient.getAllSubjects();
            System.out.println("List of schemas in Schema Registry:");

            for (String subject : subjects) {
                SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
                System.out.printf("Subject: %s, ID: %d Version: %d, Schema: %s%n", 
                                  subject, 
                                  schemaMetadata.getId(),
                                  schemaMetadata.getVersion(), 
                                  schemaMetadata.getSchema());
            }
        } catch (Exception e) {
            System.err.println("Error retrieving schemas from Schema Registry:");
            e.printStackTrace();
        }
    }
}
