package com.example.springkafka4.shareconsumer.consumer.implicit;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;

import static org.springframework.kafka.listener.ContainerProperties.*;

/**
 * In IMPLICIT mode, acknowledgment is delegated entirely to the Kafka broker,
 * which automatically accepts all acquired records regardless of processing outcome.
 * No per-record acknowledgment API is available; the ShareAcknowledgment argument is
 * always null.
 * </p>
 * This maps directly to setting share.acknowledgement.mode=implicit
 * in the Kafka client configuration.
 * </p>
 * In IMPLICIT mode the ShareConsumerRecordRecoverer is not consulted.
 * Processing errors are silently absorbed from the broker’s perspective — all records are always ACCEPTed.
 * Use this mode only when per-record delivery guarantees are not required.
 */
@Configuration
@Profile({"implicit"})
public class ImplicitShareConsumerConfiguration {

  @Bean
  public ShareKafkaListenerContainerFactory<String, TransactionEvent> implicitShareKafkaListenerContainerFactory(
      ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory) {

    ShareKafkaListenerContainerFactory<String, TransactionEvent> factory =
        new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);

    // EXPLICIT is the default
    factory.getContainerProperties().setShareAckMode(ShareAckMode.IMPLICIT);

    // Set the concurrency (default is 1)
    factory.setConcurrency(3);

    // Current limitations: Tracing is not supported yet!
    factory.getContainerProperties().setObservationEnabled(true);
    return factory;
  }
}
