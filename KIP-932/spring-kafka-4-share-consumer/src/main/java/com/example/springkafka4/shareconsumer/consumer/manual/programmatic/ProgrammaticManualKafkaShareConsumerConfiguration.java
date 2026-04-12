package com.example.springkafka4.shareconsumer.consumer.manual.programmatic;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.service.TransactionEventService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;

import static com.example.springkafka4.shareconsumer.consumer.implicit.programmatic.ProgrammaticImplicitKafkaShareConsumerConfiguration.TOPIC_NAME;
import static org.springframework.kafka.listener.ContainerProperties.*;

@Configuration
@Profile("programmatic & manual")
public class ProgrammaticManualKafkaShareConsumerConfiguration {

    @Bean
    public ProgrammaticManualKafkaShareConsumer programmaticExplicitKafkaShareConsumer(
            TransactionEventService transactionEventService) {
      return new ProgrammaticManualKafkaShareConsumer(transactionEventService);
    }

    @Bean
    public ShareKafkaMessageListenerContainer<String, TransactionEvent> programmaticExplicitProcessingContainer(
        ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory, ProgrammaticManualKafkaShareConsumer programmaticManualKafkaShareConsumer) {

      ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);
      containerProperties.setClientId("my-client-id-manual");
      containerProperties.setGroupId("programmatic-manual-share-group-processors");

        // EXPLICIT is the default
      containerProperties.setShareAckMode(ShareAckMode.MANUAL);

      // TODO Show during demo: limitations no tracing yet...
      containerProperties.setObservationEnabled(true);

      ShareKafkaMessageListenerContainer<String, TransactionEvent> container =
          new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

      container.setConcurrency(3);

      container.setupMessageListener(programmaticManualKafkaShareConsumer);
      return container;
    }
}
