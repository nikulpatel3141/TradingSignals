package com.examples.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerExample {
    public static void main(String[] args) {
        // Set up the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("Messages", "key", "Hello, Kafka!");

        // Send the record
        producer.send(record);

        // Close the producer
        producer.close();
    }
}