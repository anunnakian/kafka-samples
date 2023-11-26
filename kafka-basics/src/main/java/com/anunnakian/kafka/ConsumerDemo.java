package com.anunnakian.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "java-kafka-basics");
        properties.setProperty("auto.offset.reset", "earliest"); // none/earliest/latest

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("certif"));

            while (true) {
                LOGGER.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (var record : records) {
                    LOGGER.info("Key : " + record.key() + " | Value : " + record.value());
                    LOGGER.info("Partition : " + record.partition() + " | Offset : " + record.offset());
                }
            }
        }
    }
}