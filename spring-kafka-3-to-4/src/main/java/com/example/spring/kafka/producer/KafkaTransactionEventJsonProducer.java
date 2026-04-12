package com.example.spring.kafka.producer;


import com.example.spring.kafka.event.TransactionEventJson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaTransactionEventJsonProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionEventJsonProducer.class);

    private final String topicName;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<String, TransactionEventJson> kafkaTemplate;

    public KafkaTransactionEventJsonProducer(@Value("${topic.name}") String topicName, ObjectMapper objectMapper, KafkaTemplate<String, TransactionEventJson> kafkaTemplate) {
        this.topicName = topicName;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produce(TransactionEventJson transactionEventJson) {
        Message<TransactionEventJson> msg = MessageBuilder
                .withPayload(transactionEventJson)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
                .build();

        CompletableFuture<SendResult<String, TransactionEventJson>> send = kafkaTemplate.send(msg);

        try {
            SendResult<String, TransactionEventJson> sendResult = send.get();

            int partition = sendResult.getRecordMetadata().partition();
            long offset = sendResult.getRecordMetadata().offset();

            log.info("\uD83D\uDCE4 Produced a message successfully Partition: {}, Offset: {}: {}", partition, offset, objectMapper.writeValueAsString(transactionEventJson));
        } catch (ExecutionException | InterruptedException exception) {
            log.error(exception.getMessage());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
