package com.example.springkafka4.shareconsumer.consumer.manual.programmatic;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.exceptions.RecoverableException;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingShareConsumerAwareMessageListener;
import org.springframework.kafka.support.ShareAcknowledgment;

public class ProgrammaticManualKafkaShareConsumer implements AcknowledgingShareConsumerAwareMessageListener<String, TransactionEvent> {

  private static final Logger log = LoggerFactory.getLogger(ProgrammaticManualKafkaShareConsumer.class);
  private final TransactionEventService transactionEventService;

  public ProgrammaticManualKafkaShareConsumer(TransactionEventService transactionEventService) {
    this.transactionEventService = transactionEventService;
  }

  @Override
  public void onShareRecord(ConsumerRecord<String, TransactionEvent> record, @Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
    log.info("Processing event from topic: {} partition: {} offset: {} thread: {} value: {}",
        record.topic(),
        record.partition(),
        record.offset(),
        Thread.currentThread().getName(),
        record.value());

      try {
      transactionEventService.process(record.value());
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
