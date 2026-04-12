package com.example.springkafka4.shareconsumer.consumer.implicit.annotation;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile({"annotation & implicit"})
public class AnnotationDrivenImplicitKafkaShareConsumer {

  private static final Logger log = LoggerFactory.getLogger(AnnotationDrivenImplicitKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public AnnotationDrivenImplicitKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  @KafkaListener(
      id = "transaction-events-processor",
      topics = "transaction-events",
      containerFactory = "implicitShareKafkaListenerContainerFactory",
      groupId = "annotation-driven-implicit-share-group"
  )
  public void handleEvent(ConsumerRecord<String, TransactionEvent> consumerRecord) {
    transactionEventService.process(consumerRecord.value());
    log.info("✅ Processed event from topic: {} partition: {} offset: {} thread: {} value: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), Thread.currentThread().getName(), consumerRecord.value());

    // Implicit: acknowledgment is delegated entirely to the Kafka broker, which automatically accepts all acquired
    // records regardless of processing outcome
  }
}
