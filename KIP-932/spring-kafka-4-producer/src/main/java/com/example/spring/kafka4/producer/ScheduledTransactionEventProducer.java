package com.example.spring.kafka4.producer;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.spring.kafka4.producer.generator.RandomTransactionEventGenerator;
import java.util.concurrent.ExecutionException;
import org.springframework.scheduling.annotation.Scheduled;

public class ScheduledTransactionEventProducer {

  private final TransactionEventProducer transactionEventProducer;
  private final RandomTransactionEventGenerator generator;

  public ScheduledTransactionEventProducer(TransactionEventProducer transactionEventProducer,
                                           RandomTransactionEventGenerator generator) {
    this.transactionEventProducer = transactionEventProducer;
    this.generator = generator;
  }

  @Scheduled(fixedRateString = "${kafka.producer.rate-ms}")
  public void produce() throws ExecutionException, InterruptedException {
    TransactionEvent transactionEvent = generator.generate();
    transactionEventProducer.produce(transactionEvent);
  }
}
