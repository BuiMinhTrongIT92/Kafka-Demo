package com.trong.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ProducerService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    //Send nhung khong cho ket qua tra ve
    public void sendMessage(String topicName, String message) {
        kafkaTemplate.send(topicName, message);
    }

    //Uu tien toc do nen co the gui Async khi nao co ket qua tra ve se xu ly
    public void sendMessageAsyncReturnResult(String topicName, String message) {
        CompletableFuture<SendResult<String, String>> result = kafkaTemplate.send(topicName, message);
        result.whenComplete((item, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + item.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }


}
