
package com.example.springkafka4.shareconsumer.consumer.explicit.annotation;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile({"default", "annotation & explicit"})
public class AnnotationDrivenExplicitKafkaShareConsumer {

  private static final Logger log = LoggerFactory.getLogger(AnnotationDrivenExplicitKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public AnnotationDrivenExplicitKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  @KafkaListener(
      id = "annotation-explicit-transaction-event-processor",
      topics = "transaction-events",
      containerFactory = "explicitShareKafkaListenerContainerFactory",
      groupId = "annotation-driven-explicit-share-group"
//      ,ackMode = "EXPLICIT" // Override factory default. Enable manual acknowledgment mode
//      ,concurrency = "5" // Override factory default
  )
  public void handleEvent(ConsumerRecord<String, TransactionEvent> consumerRecord) {
    transactionEventService.process(consumerRecord.value());
    log.info("✅ Processed event from topic: {} partition: {} offset: {} thread: {} value: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), Thread.currentThread().getName(), consumerRecord.value());

    // Container sends ACCEPT on success; recoverer decides on error (default: REJECT)
  }
}
