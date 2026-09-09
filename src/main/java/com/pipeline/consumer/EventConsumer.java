package com.pipeline.consumer;

import java.util.Arrays;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pipeline.model.AddToCartEvent;
import com.pipeline.model.ClickEvent;
import com.pipeline.model.ProductViewEvent;
import com.pipeline.model.PurchaseEvent;
import com.pipeline.model.RemoveFromCartEvent;
import com.pipeline.model.SearchEvent;
import com.pipeline.serialization.EventDeserializer;

public class EventConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "clickstream-consumer-group";
    private static final String TOPIC = "clickstream-events";
    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private static String describe(ClickEvent event) {
        return switch (event) {
            case ProductViewEvent e -> "Product viewed: " + e;
            case AddToCartEvent e -> "Added to cart: " + e;
            case RemoveFromCartEvent e -> "Removed from cart: " + e;
            case SearchEvent e -> "Search performed: " + e;
            case PurchaseEvent e -> "Purchase completed: " + e;
        };
    }

    public static void main(String[] args) {
        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, ClickEvent> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, stopping event consumption...");
            consumer.wakeup();

            // Join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            // Subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(TOPIC));

            // Poll for new data
            while (true) {
                ConsumerRecords<String, ClickEvent> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, ClickEvent> record : records) {
                    log.info(describe(record.value())
                            + " (Key: " + record.key()
                            + ", Partition: " + record.partition()
                            + ", Offset: " + record.offset() + ")");
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
            // Ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // This will also commit the offsets if need be
            log.info("The consumer is now gracefully closed.");
        }
    }
}