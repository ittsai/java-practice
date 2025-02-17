package com.kafka.demo.service;

import org.springframework.kafka.core.KafkaTemplate;

public class MessageProducer {

    private static final String TOPIC = "test-topic";
    private KafkaTemplate<String, String> kafkaTemplate;

    MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        this.kafkaTemplate.send(TOPIC, message);
    }
}
