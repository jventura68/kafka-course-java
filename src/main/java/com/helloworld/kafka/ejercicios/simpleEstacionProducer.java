package com.helloworld.kafka.ejercicios;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.lang.Thread;

public class simpleEstacionProducer {
    public static void main(String[] args) throws InterruptedException {

              Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        
        final String topic = "temperatura";

        String[] estaciones = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        final Producer<String, String> producer = new KafkaProducer<>(props);
            
        final Random rnd = new Random();
        final Long numMessages = 10L;
        while(true){
            String estacion = estaciones[rnd.nextInt(estaciones.length)];
            String temperatura = Integer.toString(rnd.nextInt(35));
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, estacion, temperatura);
            producer.send(producerRecord);
            Thread.sleep(5000);
        }
        //System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        
        //producer.close();  
    }

}
