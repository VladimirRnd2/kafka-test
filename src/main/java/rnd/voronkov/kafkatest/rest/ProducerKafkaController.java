package rnd.voronkov.kafkatest.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import rnd.voronkov.kafkatest.config.KafkaConfig;
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
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaReactiveConsumer kafkaReactiveConsumer;

    @Autowired
    public ProducerKafkaController(KafkaTemplate<String, Object> kafkaTemplate, KafkaReactiveConsumer kafkaReactiveConsumer) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaReactiveConsumer = kafkaReactiveConsumer;
    }


    @PostMapping(value = "/create")
    public Flux<Goodbye> createKafkaMessage(@RequestBody Greeting message) throws JsonProcessingException {
        sendGoodbye(message);
        while (true) {
            if (kafkaReactiveConsumer.getResultMap().containsKey(message.getId().toString())) {
                List<Goodbye> goodbyeList = objectMapper.readValue(kafkaReactiveConsumer.getResultMap().get(message.getId().toString()), new TypeReference<List<Goodbye>>() {
                });
                kafkaReactiveConsumer.getResultMap().remove(message.getId().toString());
                return Flux.fromIterable(goodbyeList);
            }
        }
    }

    private void sendGoodbye(Greeting message) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(KafkaConfig.topicRq, message.getId().toString(), message);
        kafkaTemplate.send(record);
    }
}
