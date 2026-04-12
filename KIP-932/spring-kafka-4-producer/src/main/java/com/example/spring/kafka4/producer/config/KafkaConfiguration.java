package com.example.spring.kafka4.producer.config;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.spring.kafka4.producer.ScheduledTransactionEventProducer;
import com.example.spring.kafka4.producer.TransactionEventProducer;
import com.example.spring.kafka4.producer.generator.RandomTransactionEventGenerator;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaConfiguration {

    public static final String TRANSACTION_EVENTS_TOPIC_NAME = "transaction-events";

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(TRANSACTION_EVENTS_TOPIC_NAME)
                .partitions(3)
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "kafka.producer.enabled", havingValue = "true")
    RandomTransactionEventGenerator randomTransactionGenerator() {
        return new RandomTransactionEventGenerator();
    }

    @Bean
    TransactionEventProducer transactionEventProducer(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        return new TransactionEventProducer(kafkaTemplate);
    }

    @Bean
    @ConditionalOnProperty(name = "kafka.producer.enabled", havingValue = "true")
    ScheduledTransactionEventProducer scheduledTransactionEventProducer(
            TransactionEventProducer transactionEventProducer, RandomTransactionEventGenerator generator) {
        return new ScheduledTransactionEventProducer(transactionEventProducer, generator);
    }
}
