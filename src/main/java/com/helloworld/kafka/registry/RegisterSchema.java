package com.helloworld.kafka.registry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.rest.RestService;

public class RegisterSchema {

    public static void main(String[] args) throws Exception {
        // URL del Schema Registry
        String registryUrl = "http://localhost:8085";

        // Cargar el esquema del archivo test.key.avsc
        String subject = "test-value";
        String resourceFile = "test.value.avsc";
        String schemaString = readFileFromResources(resourceFile);

        // Crear un RestService para interactuar con el Schema Registry
        RestService restService = new RestService(registryUrl);
        int schemaId = restService.registerSchema(schemaString, subject);

        System.out.printf("El esquema se ha registrado con éxito bajo el sujeto '%s' y el ID %d%n", subject, schemaId);
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
