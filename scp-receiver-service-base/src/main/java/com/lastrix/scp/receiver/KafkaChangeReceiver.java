package com.lastrix.scp.receiver;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class KafkaChangeReceiver<T> implements ChangeReceiver<T> {
    private static final Logger log = LoggerFactory.getLogger(KafkaChangeReceiver.class);

    private final Consumer<String, String> consumer;
    private final Function<String, T> parser;

    public KafkaChangeReceiver(Consumer<String, String> consumer, Function<String, T> parser, String topic) {
        this.consumer = consumer;
        this.parser = parser;
        consumer.subscribe(List.of(topic));
    }

    @Override
    public ChangeChunk<T> receive(int maxSize, Duration duration) {
        var timer = Time.SYSTEM.timer(duration);
        List<T> changes = new ArrayList<>();
        Map<TopicPartition, OffsetAndMetadata> slab = new HashMap<>();
        // we should try to receive as many messages as possible in one go
        // this will help us afterwards by reducing commit calls to kafka
        while (timer.notExpired() && changes.size() < maxSize) {
            timer.update();
            var rs = consumer.poll(timer.remainingMs());
            for (ConsumerRecord<String, String> r : rs) {
                var v = parser.apply(r.value());
                // parser may fail to parse value and return null
                if (v != null) {
                    changes.add(v);
                }
                // for each message we must update our slab, that holds info
                // about each partition offset for commit
                slab.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
            }
        }
        return new ChangeChunk<>(changes, new Slab(slab));
    }

    @Override
    public void commit(Object slab) {
        if (slab instanceof Slab) {
            consumer.commitSync(((Slab) slab).map);
        } else {
            log.error("Slab is not Map of topic partition offset metadata: {}", slab == null ? "null" : slab.getClass().getTypeName());
        }
    }

    private record Slab(Map<TopicPartition, OffsetAndMetadata> map) {
    }

}
