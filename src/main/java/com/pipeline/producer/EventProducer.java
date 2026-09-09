package com.pipeline.producer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pipeline.model.ClickEvent;
import com.pipeline.serialization.EventSerializer;

public class EventProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "clickstream-events";
    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class.getName());

        // Create producer
        KafkaProducer<String, ClickEvent> producer = new KafkaProducer<>(properties);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, stopping event generation...");
            running = false;
        }));

        // Simulate live clickstream events
        Random random = new Random();
        try {
            while (running) {
                ClickEvent event = FakeEventGenerator.generateRandomEvent(random);

                // Create producer record
                ProducerRecord<String, ClickEvent> record = new ProducerRecord<>(TOPIC, event.anonymousId(), event);

                // Send data (asynchronous)
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        log.info("Sent " + event.getClass().getSimpleName()
                                + " to partition " + metadata.partition()
                                + " at offset " + metadata.offset());
                    }
                });

                // Simulate realistic-ish traffic pacing
                Thread.sleep(ThreadLocalRandom.current().nextInt(200, 800));
            }
        } catch (InterruptedException e) {
            log.info("Producer loop interrupted, shutting down.");
        } finally {
            // Flush data and close producer (synchronous)
            producer.flush();
            producer.close();
            log.info("Producer closed gracefully.");
        }
    }
}
