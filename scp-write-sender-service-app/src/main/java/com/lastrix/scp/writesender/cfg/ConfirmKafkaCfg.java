package com.lastrix.scp.writesender.cfg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lastrix.scp.receiver.ChangeReceiver;
import com.lastrix.scp.receiver.KafkaChangeReceiver;
import com.lastrix.scp.writesender.model.EnrolleeSelectId;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ConfirmKafkaCfg {
    private static final Logger log = LoggerFactory.getLogger(ConfirmKafkaCfg.class);

    private static final int ONE_KB = 1024;
    private static final int ONE_MB = 1024 * ONE_KB;
    @Value("${scp.kafka.url}")
    private String kafkaURL;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConsumerProperties());
    }

    private Map<String, Object> kafkaConsumerProperties() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "write-sender");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // disabled for error handling
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        cfg.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
//        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, ONE_MB); //1mb
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        cfg.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 256 * ONE_KB); //256kb
        cfg.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 8 * ONE_MB); // 8 mb receive buffer
        cfg.put(ConsumerConfig.SEND_BUFFER_CONFIG, 8 * ONE_MB); // 8 mb send buffer
        cfg.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
        cfg.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
        return cfg;
    }

    @Bean
    public ChangeReceiver<EnrolleeSelectId> enrolleeSelectIdChangeReceiver(
            ConsumerFactory<String, String> factory,
            ObjectMapper mapper,
            @Value("${scp.kafka.topics.confirm}") String topic) {
        return new KafkaChangeReceiver<>(factory.createConsumer(), x -> parseSafe(mapper, x), topic);
    }

    private EnrolleeSelectId parseSafe(ObjectMapper mapper, String x) {
        try {
            return mapper.readValue(x, EnrolleeSelectId.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse from json: {}", x, e);
            return null;
        }
    }
}
