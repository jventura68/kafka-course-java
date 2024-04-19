# Guía Rápida de Kafka

A continuación, se describe una guía rápida de cómo trabajar con Kafka para definir un topic, lanzar un consumer y un producer.

Para poder incluir los comandos debemos abrir un terminal sobre el docker 
* Para abrir un terminal en el contenedor:


   
   ```bash
   docker exec -it docker-kafka1-1 bash
   ```

## 1. Definir un Topic

Para definir un topic en Kafka con una única partición, utiliza el siguiente comando:

```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic miTopic
```

A continuación podemos listar los topics que existen en el cluster
```bash
kafka-topics --list --bootstrap-server localhost:9092

```

## Definir consumidor y productor

Podemos abrir dos terminales para sobre contenedor docker-kafka-1. En uno de ellos ejecutamos el consumer y en el otro el producer

* El consumidor en uno de los terminales
  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 --topic mitopic
  ```
* El productor en el otro
  ```bash
  kafka-console-producer --broker-list localhost:9092 --topic mitopic
  ```

* Leer todos los mensajes y no solo los que se creen nuevos
  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-test --from-beginning
  ```

* Borrado de topics

```bash
kafka-topics --bootstrap-server localhost:9092 --topic topic-test2 --create --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server localhost:9092 --topic topic-test2 --delete
kafka-topics --bootstrap-server localhost:9092 --list
```


* Enviando mensajes sin clave, se reparten por round robin por las particiones
  Los consumidores se pueden arrancar y parar, se rebalancea la asignación de particiones
```bash
kaftopics --bootstrap-server localhost:9092 --topic topic-no-key --create --partitions 2 --replication-factor 1
kafka-console-producer --broker-list localhost:9092 --topic topic-no-key
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-no-key --from-beginning --group "no-key"
```

* Enviando mensajes con clave
```bash
kafka-topics --bootstrap-server localhost:9092 --topic topic-key --create --partitions 3 --replication-factor 1

kafka-console-producer --broker-list localhost:9092 --topic topic-key --property "parse.key= true" --property "key.separator=:"
```

* verificar que los mensajes se enrutan a las particiones en función de la clave. Abrir cada consumer en un terminal de docker-kafka1 distinto.

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-key --from-beginning --partition 0 --property "print.key=true"
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-key --from-beginning --partition 1 --property "print.key=true"
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-key --from-beginning --partition 2 --property "print.key=true"
```
