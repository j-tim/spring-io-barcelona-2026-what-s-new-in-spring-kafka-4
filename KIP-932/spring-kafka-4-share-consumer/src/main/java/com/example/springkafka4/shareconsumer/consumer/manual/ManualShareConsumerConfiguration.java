package com.example.springkafka4.shareconsumer.consumer.manual;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.consumer.manual.annotation.AnnotationDrivenManualKafkaShareConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;

import java.time.Duration;

import static org.springframework.kafka.listener.ContainerProperties.*;

/**
 * In MANUAL mode the listener drives every per-record acknowledgment decision. Each record is delivered with
 * a non-null {@link org.springframework.kafka.support.ShareAcknowledgment} instance that the listener must call
 * exactly once with terminal operation (acknowledge(), release(), or reject()).
 * </p>
 * Subsequent polls are blocked until all records from the previous poll are acknowledged.
 * Use MANUAL mode when your business logic determines the acknowledgment outcome record by record.
 * </p>
 * Used by {@link AnnotationDrivenManualKafkaShareConsumer}
 */
@Profile({"manual"})
@Configuration
public class ManualShareConsumerConfiguration {

    @Bean
    public ShareKafkaListenerContainerFactory<String, TransactionEvent> manualShareKafkaListenerContainerFactory(
            ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory) {

        ShareKafkaListenerContainerFactory<String, TransactionEvent> factory =
                new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);

        // EXPLICIT is the default. In this demo we want to have more control, so we set it to manual
        factory.getContainerProperties().setShareAckMode(ShareAckMode.MANUAL);

        // Set the concurrency: default value 1. We can scale beyond the number of partitions!
        factory.setConcurrency(6);

        // Help identify missing acknowledgments
        // When a record is not acknowledged within the specified timeout, a warning is logged with details about the unacknowledged record.
        factory.getContainerProperties().setShareAcknowledgmentTimeout(Duration.ofSeconds(30));

        // Default is true: Durable — acknowledgments are guaranteed persisted before the next poll
        // Set if to false for a higher throughput. With the slight risk of redelivery if the consumer crashes before
        // the async commit completes!
        factory.getContainerProperties().setSyncShareCommits(true);

        // Current limitations: Tracing is not supported yet!
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}