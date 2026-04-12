package com.example.springkafka4.shareconsumer.consumer.explicit.programmatic;

import static com.example.springkafka4.shareconsumer.consumer.implicit.programmatic.ProgrammaticImplicitKafkaShareConsumerConfiguration.TOPIC_NAME;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;

@Configuration
@Profile({"programmatic & explicit"})
public class ProgrammaticExplicitKafkaShareConsumerConfiguration {

    @Bean
    public ProgrammaticExplicitKafkaShareConsumer programmaticExplicitKafkaShareConsumer(
            TransactionEventService transactionEventService) {
      return new ProgrammaticExplicitKafkaShareConsumer(transactionEventService);
    }

    @Bean
    public ShareKafkaMessageListenerContainer<String, TransactionEvent> programmaticExplicitProcessingContainer(
        ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory, ProgrammaticExplicitKafkaShareConsumer programmaticExplicitKafkaShareConsumer) {

      ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);
      containerProperties.setClientId("my-client-id-explicit");
      containerProperties.setGroupId("programmatic-explicit-share-group-processors");

        // EXPLICIT is the default — no shareAckMode configuration needed
//      containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);

      containerProperties.setObservationEnabled(true);

      ShareKafkaMessageListenerContainer<String, TransactionEvent> container =
          new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

      container.setConcurrency(3);

      container.setupMessageListener(programmaticExplicitKafkaShareConsumer);
      return container;
    }
}
