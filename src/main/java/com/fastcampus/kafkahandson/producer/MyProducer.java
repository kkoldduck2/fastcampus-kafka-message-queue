package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.model.MyMessage;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.function.Supplier;

@Component
public class MyProducer implements Supplier<Flux<Message<MyMessage>>> {

    MyProducer() {
        System.out.println("MyCosumer init!");
    }

    private final Sinks.Many<Message<MyMessage>> sinks = Sinks.many().unicast().onBackpressureBuffer();

    public void sendMessage(MyMessage myMessage) {
        Message<MyMessage> message = MessageBuilder
                .withPayload(myMessage)
                .setHeader(KafkaHeaders.KEY, String.valueOf(myMessage.getAge())) // age 값을 Key로 사용
                // 같은 age면 항상 같은 파티션으로 감
                .build();
        sinks.emitNext(message, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    @Override
    public Flux<Message<MyMessage>> get() {
        return sinks.asFlux();
    }
}
