package com.kosta.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

public class JavaProducer {
    
    //@Value("${spring.kafka.bootstrap-servers}")
	static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "MYTOPIC";
        String message = "Hello, Kafka!";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        producer.send(record);

        producer.close();
    }
}