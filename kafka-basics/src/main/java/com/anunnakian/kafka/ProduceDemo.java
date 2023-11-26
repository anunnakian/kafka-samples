package com.anunnakian.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceDemo.class.getSimpleName());

    public static void main(String[] args) {
        LOGGER.info("Hello World");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "54.195.186.50:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Hello Jilali");
            producer.send(record);
        }
    }
}