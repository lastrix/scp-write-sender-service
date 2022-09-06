package com.lastrix.scp.writesender.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaChangeSender<T> implements ChangeSender<T> {
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
        for (T change : changes) {
            kafkaTemplate.send(topic, toJson(change))
                    .completable()
                    .thenApply(r -> {
                        results.add(change);
                        latch.countDown();
                        return true;
                    })
                    .exceptionally(throwable -> {
                        latch.countDown();
                        return false;
                    });
        }
        // actually this may lock thread for much longer than
        // we would think because kafka may send messages only
        // when certain amount of bytes could be sent to server
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
