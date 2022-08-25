package rnd.voronkov.kafkatest.service;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Component
@Data
public class KafkaReactiveConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReactiveConsumer.class);


    private final KafkaReceiver<String, String> kafkaReceiver;
    private final Map<String, String> resultMap = new ConcurrentHashMap<>();

    @Autowired
    public KafkaReactiveConsumer(KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;
    }

    @EventListener(ContextRefreshedEvent.class)
    public Disposable getFlux() {
        return kafkaReceiver.receive().checkpoint().log()
                .doOnNext(r -> {
                    resultMap.put(r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .map(ConsumerRecord::value)
                .subscribeOn(Schedulers.newParallel("lol"))
                .subscribe();
    }
}
