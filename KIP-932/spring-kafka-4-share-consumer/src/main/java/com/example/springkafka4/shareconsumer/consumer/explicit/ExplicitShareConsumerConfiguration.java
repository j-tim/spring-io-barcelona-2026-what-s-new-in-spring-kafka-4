package com.example.springkafka4.shareconsumer.consumer.explicit;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;

/**
 * In EXPLICIT mode the container takes full responsibility for acknowledgment.
 * After the listener returns normally, the container sends ACCEPT for the record.
 * On listener exceptions, the {@link org.springframework.kafka.listener.ShareConsumerRecordRecoverer}
 * decides the outcome (ACCEPT, RELEASE, or REJECT; default: REJECT).
 * </p>
 * This is the closest analogue to disabling auto.commit on a regular consumer —
 * the container manages all acknowledgment decisions without any listener involvement.
 */
@Configuration
@Profile({"default", "explicit"})
public class ExplicitShareConsumerConfiguration {

  @Bean
  public ShareKafkaListenerContainerFactory<String, TransactionEvent> explicitShareKafkaListenerContainerFactory(
      ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory) {

    ShareKafkaListenerContainerFactory<String, TransactionEvent> factory =
        new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);

    // EXPLICIT is the default — no shareAckMode configuration needed
//    factory.getContainerProperties().setShareAckMode(ShareAckMode.EXPLICIT);

    // Set the concurrency
    factory.setConcurrency(3);

    factory.getContainerProperties().setObservationEnabled(true);
    return factory;
  }
}
