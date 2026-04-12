package com.example.spring.kafka.producer;

import com.example.spring.kafka.event.TransactionEventJson;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@EnableScheduling
@Component
@ConditionalOnProperty(
    name = "kafka.producer.scheduled.enabled", havingValue = "true"
)
public class ScheduledProducer {

    private final KafkaTransactionEventJsonProducer kafkaTransactionEventJsonProducer;
    private final TransactionEventGenerator transactionEventGenerator;

    public ScheduledProducer(KafkaTransactionEventJsonProducer kafkaTransactionEventJsonProducer, TransactionEventGenerator transactionEventGenerator) {
        this.kafkaTransactionEventJsonProducer = kafkaTransactionEventJsonProducer;
        this.transactionEventGenerator = transactionEventGenerator;
    }

    @Scheduled(fixedRate = 5000)
    public void keepProducing() {
        TransactionEventJson event = transactionEventGenerator.generate();
        kafkaTransactionEventJsonProducer.produce(event);
    }
}
