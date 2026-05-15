package com.lavish.springappusingkafka.controller;


import com.lavish.springappusingkafka.DTO.messageRequest;
import com.lavish.springappusingkafka.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/message")
public class MessageController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/checkhealth")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Kafka Server is up and running");
    }

    @PostMapping("/sendmessage")
    public ResponseEntity<String> sendMessage(@RequestBody messageRequest messagerequest) {
        String messageId = UUID.randomUUID().toString();

        kafkaProducerService.sendMessage(messageId, messagerequest.getMessagecontent(), messagerequest.getSender());

        return ResponseEntity.ok("Message sent with Id: " + messageId);
    }
}
