package com.fastcampus.kafkahandson.consumer;

import ch.qos.logback.core.net.SyslogOutputStream;
import com.fastcampus.kafkahandson.model.MyMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class MyConsumer {

    @KafkaListener(
            topics = { "my-json-topic" },
            groupId = "test-consumer-group"
    )
    public void accept(ConsumerRecord<String, MyMessage> message) {
        // 이 컨슈머의 역할은 메시지를 받아서 그 메시지를 그냥 프린트로 찍어주는 역할만 한다.
        System.out.println("[Main Consumer] Message arrived! - " + message.value());
        // TODO message.getHeaders() 도 찍어보기
    }
}
