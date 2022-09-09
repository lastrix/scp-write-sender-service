package com.lastrix.scp.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaChangeSender<T> implements ChangeSender<T> {
    private static final Logger log = LoggerFactory.getLogger(KafkaChangeSender.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;
    private final String topicTemplate;

    public KafkaChangeSender(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper, String topicTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.topicTemplate = topicTemplate;
    }

    @Override
    public List<T> send(List<T> changes, int channel) {
        var topic = topicTemplate + channel;
        List<T> results = Collections.synchronizedList(new ArrayList<>(changes.size()));
        CountDownLatch latch = new CountDownLatch(changes.size());
        // we should map messages before send
        // this way we may be sure that in case of json serialization exception
        // no message sent to kafka
        var list = changes.stream().map(x -> ImmutablePair.of(x, toJson(x))).toList();
        // essentially we are trying to send as many messages as possible, this method
        // should be called with multiple messages to work efficiently, otherwise kafka
        // may refuse to send them right away because buffer is not full enough or not
        // enough time passed from last send
        for (var p : list) {
            kafkaTemplate.send(topic, p.getValue())
                    .completable()
                    .thenApply(r -> {
                        log.trace("Wrote message to {}:{}", r.getProducerRecord().topic(), r.getProducerRecord().partition());
                        results.add(p.getKey());
                        latch.countDown();
                        return true;
                    })
                    .exceptionally(throwable -> {
                        latch.countDown();
                        return false;
                    });
        }
        awaitLatch(latch);
        return results;
    }

    private String toJson(T change) {
        try {
            return mapper.writeValueAsString(change);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to convert value to json", e);
        }
    }

    private void awaitLatch(CountDownLatch latch) {
        // we should wait till all changes sent to server
        // it should happen eventually
        while (latch.getCount() > 0) {
            try {
                if (latch.await(100, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException ignored) {
                // do nothing
            }
        }
    }
}
