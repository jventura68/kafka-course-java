package com.helloworld.kafka.producers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducer {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
    public static void main(final String[] args) throws IOException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        final String topic = "test-topic";

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        final Producer<String, String> producer = new KafkaProducer<>(props);
            
        final Random rnd = new Random();
        final Long numMessages = 10L;
        for (Long i = 0L; i < numMessages; i++) {
            String user = users[rnd.nextInt(users.length)];
            String item = items[rnd.nextInt(items.length)];
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, user, item);
            producer.send(producerRecord,new ProducerCallback());
        }
        System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        
        producer.close();

    }
    
    public static void getFutureRecordMetadata(String user, String item,RecordMetadata metadata, Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            System.out.printf("Produced event to topic %s: user= %-10s value = %-20s partition=%d%n", 
            					metadata.topic(), user, item, metadata.partition());
    }
    
}

class ProducerCallback implements Callback {
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			System.out.printf("Produced event to topic %s offset= %d partition=%d%n", 
					metadata.topic(), metadata.offset(), metadata.partition());
		} else {
			exception.printStackTrace();
		}
		
	}
}

