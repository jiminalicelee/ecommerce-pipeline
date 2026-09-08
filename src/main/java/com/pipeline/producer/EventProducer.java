package com.pipeline.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "clickstream-events";
    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, "hello world");
        
        // Send data
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            log.info("Sent to partition " + recordMetadata.partition() 
                + " at offset " + recordMetadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to send record ", e);
        }

        // Flush data (synchronous)
        producer.flush();
        // Flush and close producer
        producer.close();
    }
}
