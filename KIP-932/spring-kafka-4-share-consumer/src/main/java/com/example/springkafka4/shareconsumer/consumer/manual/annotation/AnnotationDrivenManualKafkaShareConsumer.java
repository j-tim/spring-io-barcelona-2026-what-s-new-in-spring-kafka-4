
package com.example.springkafka4.shareconsumer.consumer.manual.annotation;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.exceptions.RecoverableException;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("annotation & manual")
public class AnnotationDrivenManualKafkaShareConsumer {

  private static final Logger log = LoggerFactory.getLogger(AnnotationDrivenManualKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public AnnotationDrivenManualKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  /**
   * Be aware!
   * Failing to acknowledge records will block further message processing.
   * Always ensure records are acknowledged in all code paths, including exception handlers.
   */
  @KafkaListener(
      id = "annotation-manual-transaction-event-processor",
      topics = "transaction-events",
      containerFactory = "manualShareKafkaListenerContainerFactory",
      groupId = "annotation-driven-manual-share-group"
//      ackMode = "MANUAL" // Override factory default.
//      ,concurrency = "5" // Override factory default
  )
  public void handleEvent(ConsumerRecord<String, TransactionEvent> consumerRecord, ShareAcknowledgment acknowledgment) {
    log.info("Processing event from topic: {} partition: {} offset: {} thread: {} value: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), Thread.currentThread().getName(), consumerRecord.value());

    TransactionEvent event = consumerRecord.value();

    try {
      transactionEventService.process(event);

      acknowledgment.acknowledge();
      log.info("✅ Acknowledged: Handled event successfully!");

      // TODO Show during demo: Renew will become available in Spring Kafka 4.1 (Spring Boot 4.1.x)
      // To extend the acquisition lock when processing exceeds the broker’s lock duration
//      acknowledgment.renew();
    } catch (RecoverableException e) {
      // Release for retry with another consumer
      acknowledgment.release();
      log.warn("🔄 Released for retry: Can't handle event now. But it might succeed in another try");
    } catch (Exception e) {
      // Permanently reject un-recoverable events
      acknowledgment.reject();
      log.warn("❌ Rejected: Failed permanently. Don't try again!");
    }
  }
}
