package com.pipeline.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class EventProducer {
    private static final String TOPIC = "clickstream-events";
    private static final String BOOTSTRAPSERVERS = "localhost:9092";
    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, "hello world");
        
        // Send data (asynchronous)
        producer.send(record);

        // Flush data (synchronous)
        producer.flush();
        // Flush and close producer
        producer.close();
    }
}
