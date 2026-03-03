package com.fastcampus.kafkahandson.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_SECOND_TOPIC;

@RequiredArgsConstructor
@Component
public class MySecondProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessageWithKey(String key, String myMessage) {
        kafkaTemplate.send(MY_SECOND_TOPIC, key, myMessage);
    }
}

