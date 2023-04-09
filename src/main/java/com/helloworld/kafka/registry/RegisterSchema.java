package com.helloworld.kafka.registry;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RegisterSchema {

    public static void main(String[] args) throws Exception {
        // URL del Schema Registry
        String registryUrl = "http://localhost:8085";

        // Cargar el esquema del archivo test.key.avsc
        String avroSchemaFilePath = "src/main/resources/test.key.avsc";
        String keySchemaString = readFileFromResources(avroSchemaFilePath);

        // Crear un RestService para interactuar con el Schema Registry
        RestService restService = new RestService(registryUrl);

        // Registrar el esquema en el Schema Registry
        String subject = "test-key";
        int schemaId = registerSchema(restService, subject, keySchemaString);

        System.out.printf("El esquema se ha registrado con éxito bajo el sujeto '%s' y el ID %d%n", subject, schemaId);
    }

    private static int registerSchema(RestService restService, String subject, String schemaString) throws Exception {
        Schema schema = new Schema(subject, schemaString, "AVRO");
        return restService.registerSchema(schema);
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
