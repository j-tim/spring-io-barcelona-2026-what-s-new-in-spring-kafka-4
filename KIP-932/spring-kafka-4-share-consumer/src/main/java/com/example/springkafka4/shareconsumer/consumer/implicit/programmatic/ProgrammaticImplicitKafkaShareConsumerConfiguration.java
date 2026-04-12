package com.example.springkafka4.shareconsumer.consumer.implicit.programmatic;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;
import org.springframework.kafka.support.ShareAcknowledgment;

@Configuration
@Profile({"programmatic & implicit"})
public class ProgrammaticImplicitKafkaShareConsumerConfiguration {

  public static final String TOPIC_NAME = "transaction-events";

  @Bean
  public ProgrammaticImplicitKafkaShareConsumer programmaticKafkaShareConsumer(TransactionEventService transactionEventService) {
    return new ProgrammaticImplicitKafkaShareConsumer(transactionEventService);
  }

  /**
   * The {@link ShareAcknowledgment} acknowledgment parameter is non-null only in MANUAL mode;
   * it is null in EXPLICIT and IMPLICIT modes.
   */
  @Bean
  public ShareKafkaMessageListenerContainer<String, TransactionEvent> programmaticProcessingContainer(
      ShareConsumerFactory<String, TransactionEvent> implicitShareKafkaListenerContainerFactory, ProgrammaticImplicitKafkaShareConsumer programmaticImplicitKafkaShareConsumer) {

    ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);
    containerProperties.setClientId("my-client-id");
    containerProperties.setGroupId("programmatic-implicit-share-group-processors");

    ShareKafkaMessageListenerContainer<String, TransactionEvent> container =
        new ShareKafkaMessageListenerContainer<>(implicitShareKafkaListenerContainerFactory, containerProperties);

    container.getContainerProperties().setShareAckMode(ContainerProperties.ShareAckMode.IMPLICIT);
    container.setConcurrency(3);

    container.setupMessageListener(programmaticImplicitKafkaShareConsumer);
    return container;
  }

}
