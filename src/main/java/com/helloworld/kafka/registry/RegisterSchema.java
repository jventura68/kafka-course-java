package com.helloworld.kafka.registry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class RegisterSchema {

    public static void main(String[] args) throws IOException, RestClientException{
        // URL del Schema Registry
        String registryUrl = "http://localhost:8085";

        registerSchema(registryUrl, "test-key", "test.key.avsc");
        registerSchema(registryUrl, "test-value", "test.value.avsc");
        
    }
    
    public static int registerSchema(String registryUrl, String subject, String resourceFile) throws IOException, RestClientException{
        
        String schemaString = readFileFromResources(resourceFile);
    
        // Crear un RestService para interactuar con el Schema Registry
        RestService restService = new RestService(registryUrl);
        int schemaId = restService.registerSchema(schemaString, subject);
        System.out.printf("El esquema se ha registrado con éxito bajo el sujeto '%s' y el ID %d%n", subject, schemaId);
        return schemaId;
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
