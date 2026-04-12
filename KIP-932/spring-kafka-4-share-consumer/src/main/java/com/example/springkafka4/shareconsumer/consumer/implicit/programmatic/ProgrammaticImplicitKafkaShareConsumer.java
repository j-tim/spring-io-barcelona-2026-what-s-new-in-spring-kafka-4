package com.example.springkafka4.shareconsumer.consumer.implicit.programmatic;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

public class ProgrammaticImplicitKafkaShareConsumer implements MessageListener<String, TransactionEvent> {

  private static final Logger log = LoggerFactory.getLogger(ProgrammaticImplicitKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public ProgrammaticImplicitKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  @Override
  public void onMessage(ConsumerRecord<String, TransactionEvent> consumerRecord) {
    transactionEventService.process(consumerRecord.value());

    log.info("✅ Processed event from topic: {} partition: {} offset: {} thread: {} value: {}",
            consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.offset(),
            Thread.currentThread().getName(),
            consumerRecord.value());

    // Implicit: acknowledgment is delegated entirely to the Kafka broker, which automatically accepts all acquired
    // records regardless of processing outcome
  }
}
