package com.example.spring.kafka.consumer;

import com.example.spring.kafka.event.TransactionEventJson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "kafka.consumer.enabled", havingValue = "true")
public class KafkaTransactionEventJsonConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionEventJsonConsumer.class);
    private final ObjectMapper objectMapper;

    public KafkaTransactionEventJsonConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${topic.name}")
    public void on(@Payload TransactionEventJson transactionEvent) throws JsonProcessingException {
        log.info("\uD83D\uDCE5 Consumed message successfully: {}", objectMapper.writeValueAsString(transactionEvent));
    }
}
