package com.kafka.demo.controller;

import com.kafka.demo.service.MessageProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    private MessageProducer messageProducer;

    MessageController(MessageProducer messageProducer) {
        this.messageProducer  = messageProducer;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestBody String message) {
        messageProducer.sendMessage(message);

        return "Sent";
    }
}
