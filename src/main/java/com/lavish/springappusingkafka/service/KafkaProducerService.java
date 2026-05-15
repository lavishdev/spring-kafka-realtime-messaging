package com.lavish.springappusingkafka.service;


import com.lavish.springappusingkafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final String Topic = "driver-location";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    public void sendMessage(Message message) {
        logger.info("sending message to the topic: " + Topic);
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(Topic, message.getId(), message);
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                logger.error("Failed to send the message" + exception.getMessage());
            }
            else  {
                logger.info("Message sent with the offset" + result.getRecordMetadata().offset());
            }
        });
    }

    public void sendMessage(String messageId, String messagecontent, String sender) {
        Message message = new Message(messageId, messagecontent, sender);
        sendMessage(message);
    }
}
