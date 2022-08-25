package rnd.voronkov.kafkatest.service;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private KafkaTemplate<String,Object> kafkaTemplate;

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC = "bookRs";

    private final KafkaReceiver<String,String> kafkaReceiver;
    private final Map<String,String> resultMap = new ConcurrentHashMap<>();

    @Autowired
    public KafkaReactiveConsumer(KafkaTemplate<String, Object> kafkaTemplate, KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaReceiver = kafkaReceiver;
    }

    public Disposable consumeMessages(String topic, CountDownLatch latch) {
        return kafkaReceiver.receive()
                .doOnNext(r -> {
                    resultMap.put(r.key(), r.value());
                    latch.countDown();
                    r.receiverOffset().acknowledge();
                })
                .doOnError(Throwable::printStackTrace)
                .map(r -> {
                    String value = r.value();
                    System.out.println(value);
                    return value;
                })
                .publishOn(Schedulers.newParallel("lol"))
                .subscribe();
    }
}
