package com.example.spring.kafka4.producer;

import static com.example.spring.kafka4.producer.config.KafkaConfiguration.TRANSACTION_EVENTS_TOPIC_NAME;

import java.util.concurrent.ExecutionException;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

public class TransactionEventProducer {

  private static final Logger log = LoggerFactory.getLogger(TransactionEventProducer.class);

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

  public TransactionEventProducer(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void produce(TransactionEvent transactionEvent) throws ExecutionException, InterruptedException {
    SendResult<String, TransactionEvent> sendResult = kafkaTemplate.send(
            TRANSACTION_EVENTS_TOPIC_NAME, transactionEvent.getFromIban(), transactionEvent).get();

    String topic = sendResult.getRecordMetadata().topic();
    int partition = sendResult.getRecordMetadata().partition();

    log.info("Produced to topic: {}, partition: {} Avro data: {}", topic, partition, transactionEvent);
  }

}
