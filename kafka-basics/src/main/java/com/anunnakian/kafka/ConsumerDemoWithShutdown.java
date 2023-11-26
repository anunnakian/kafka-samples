package com.anunnakian.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "java-kafka-basics-group");
        //properties.setProperty("auto.offset.reset", "earliest"); // none/earliest/latest

        Thread mainThread = Thread.currentThread();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown detected.");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            consumer.subscribe(List.of("certif_key"));

            while (true) {
                LOGGER.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (var record : records) {
                    LOGGER.info("Key : " + record.key() + " | Value : " + record.value());
                    LOGGER.info("Partition : " + record.partition() + " | Offset : " + record.offset());
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Gracefully shut down the app...");
        }
    }
}