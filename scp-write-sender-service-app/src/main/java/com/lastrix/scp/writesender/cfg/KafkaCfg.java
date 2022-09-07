package com.lastrix.scp.writesender.cfg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lastrix.scp.sender.KafkaChangeSender;
import com.lastrix.scp.writesender.model.EnrolleeSelect;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaCfg {
    private static final int ONE_KB = 1024;
    private static final int ONE_MB = 1024 * ONE_KB;
    @Value("${scp.kafka.url}")
    private String kafkaURL;
    @Value("${scp.kafka.topics.direct.template}")
    private String topicTemplate;

    @Bean
    public KafkaTemplate<String, String> commonKafkaTemplate() {
        return new KafkaTemplate<>(newProducerFactory());
    }

    private ProducerFactory<String, String> newProducerFactory() {
        return new DefaultKafkaProducerFactory<>(createKafkaProducerProperties());
    }

    private Map<String, Object> createKafkaProducerProperties() {
        var cfg = new HashMap<String, Object>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        cfg.put(ProducerConfig.RETRIES_CONFIG, 10);
        cfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 16 * ONE_MB); // allow for 16 mb buffer
        cfg.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 8 * ONE_MB); // 8 mb receive buffer
        cfg.put(ProducerConfig.SEND_BUFFER_CONFIG, 8 * ONE_MB); // 8 mb send buffer
        return cfg;
    }

    @Bean
    public KafkaChangeSender<EnrolleeSelect> enrolleeSelectKafkaChangeSender(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        return new KafkaChangeSender<>(kafkaTemplate, mapper, topicTemplate);
    }
}
