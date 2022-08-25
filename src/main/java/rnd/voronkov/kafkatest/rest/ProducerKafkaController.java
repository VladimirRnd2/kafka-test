package rnd.voronkov.kafkatest.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import rnd.voronkov.kafkatest.model.Goodbye;
import rnd.voronkov.kafkatest.model.Greeting;
import rnd.voronkov.kafkatest.service.KafkaReactiveConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/kafka")
public class ProducerKafkaController {


    private KafkaTemplate<String, Object> kafkaTemplate;
    private final WebClient webClient;
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaReactiveConsumer kafkaReactiveConsumer;

    private final Map<String, String> map = new ConcurrentHashMap<>();

    @Autowired
    public ProducerKafkaController(KafkaTemplate<String, Object> kafkaTemplate, WebClient webClient, KafkaReceiver<String, String> kafkaReceiver, KafkaReactiveConsumer kafkaReactiveConsumer) {
        this.kafkaTemplate = kafkaTemplate;
        this.webClient = webClient;
        this.kafkaReceiver = kafkaReceiver;
        this.kafkaReactiveConsumer = kafkaReactiveConsumer;
    }


    @PostMapping(value = "/create")
    public Flux<Goodbye> createKafkaMessage(@RequestBody Greeting message) throws InterruptedException, ExecutionException, JsonProcessingException {
        sendGoodbye(message);
//        consumeAndDispose();
        while (true) {
            if (kafkaReactiveConsumer.getResultMap().containsKey(message.getId().toString())) {
                List<Goodbye> goodbyeList = objectMapper.readValue(kafkaReactiveConsumer.getResultMap().get(message.getId().toString()), new TypeReference<List<Goodbye>>() {});
                kafkaReactiveConsumer.getResultMap().remove(message.getId().toString());
                return Flux.fromIterable(goodbyeList);
            }
        }
    }

    private static byte[] intToBytesBigEndian(final int data) {
        return new byte[]{
                (byte) ((data >> 24) & 0xff), (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff),
        };
    }

    private void sendGoodbye(Greeting message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, Object> record = new ProducerRecord<>("bookRq", message.getId().toString(), message);
        kafkaTemplate.send(record);
    }

        @EventListener(ContextRefreshedEvent.class)
//    @GetMapping("/flux")
    public Disposable getFlux() {
        return kafkaReceiver.receive()
                .doOnNext(r -> {
                    kafkaReactiveConsumer.getResultMap().put(r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .doOnError(Throwable::printStackTrace)
                .map(r -> {
                    String value = r.value();
                    System.out.println(value);
                    return value;
                })
                .publishOn(Schedulers.newParallel("lol")).subscribe();
    }

    private void consumeAndDispose() throws InterruptedException {
        int count = 1;
        CountDownLatch latch = new CountDownLatch(count);
        Disposable disposable = kafkaReactiveConsumer.consumeMessages("bookRs", latch);
        latch.await(5, TimeUnit.SECONDS);
        disposable.dispose();
    }
}
