package com.anunnakian.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemoCallBack {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceDemoCallBack.class.getSimpleName());

    public static void main(String[] args) {
        LOGGER.info("Hello World");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("certif_partition", "Hello " + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        LOGGER.info("""
                                Receiving metadata :
                                    Topic : $topic
                                    Partition : $partition
                                    Offset : $offset
                                    Timestamp : $timestamp
                                """.replace("$topic", metadata.topic())
                                .replace("$partition", String.valueOf(metadata.partition()))
                                .replace("$offset", String.valueOf(metadata.offset()))
                                .replace("$timestamp", String.valueOf(metadata.timestamp())));
                    } else {
                        exception.printStackTrace();
                    }
                });
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}