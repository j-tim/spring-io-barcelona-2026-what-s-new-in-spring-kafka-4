package com.example.springkafka4.shareconsumer.service;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import com.example.springkafka4.shareconsumer.exceptions.RecoverableException;
import com.example.springkafka4.shareconsumer.exceptions.UnRecoverableException;
import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class TransactionEventService {

  private static final Logger log = LoggerFactory.getLogger(TransactionEventService.class);

  public void process(TransactionEvent event) {
    log.debug("Processing transaction event with amount: {}", event.getAmount());

    if (hasNegativeAmoundValue(event)) {
      log.warn("Invalid TransactionEvent, because of negative transaction amount value: {}", event.getAmount());
      throw new UnRecoverableException("We can't transaction event with a negative amount!");
    }
    
    mimicRecoverableAbleException(event);
  }

  private void mimicRecoverableAbleException(TransactionEvent event) {
    if (hasDescriptionContainingRetry(event)) {
      // 50/50 chance of throwing RecoverableException
      if (Math.random() < 0.5) {
        throw new RecoverableException("Random recoverable exception for testing purposes.");
      }
    }
  }

  private boolean hasDescriptionContainingRetry(TransactionEvent event) {
    return event.getDescription().contains("recoverable exception");
  }
  
  private boolean hasNegativeAmoundValue(TransactionEvent event) {
    return event.getAmount().compareTo(BigDecimal.ZERO) < 0;
  }
}
