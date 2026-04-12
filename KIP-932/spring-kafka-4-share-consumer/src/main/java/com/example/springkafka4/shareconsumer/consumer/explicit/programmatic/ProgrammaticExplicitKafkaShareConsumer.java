package com.example.springkafka4.shareconsumer.consumer.explicit.programmatic;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingShareConsumerAwareMessageListener;
import org.springframework.kafka.support.ShareAcknowledgment;

public class ProgrammaticExplicitKafkaShareConsumer implements AcknowledgingShareConsumerAwareMessageListener<String, TransactionEvent> {

  private static final Logger log = LoggerFactory.getLogger(ProgrammaticExplicitKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public ProgrammaticExplicitKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  /**
   * The {@link ShareAcknowledgment} acknowledgment parameter is non-null only in MANUAL mode;
   * it is null in EXPLICIT and IMPLICIT modes.
   */
  @Override
  public void onShareRecord(ConsumerRecord<String, TransactionEvent> consumerRecord, @Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
    transactionEventService.process(consumerRecord.value());
    log.info("✅ Processed event from topic: {} partition: {} offset: {} thread: {} value: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), Thread.currentThread().getName(), consumerRecord.value());

    // Container sends ACCEPT on success; recoverer decides on error (default: REJECT)
  }
}
