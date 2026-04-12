package com.example.spring.kafka4.producer.rest;

import com.example.spring.kafka4.producer.TransactionEventProducer;
import com.example.spring.kafka.avro.transactions.Currency;
import com.example.spring.kafka.avro.transactions.TransactionEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

  private final TransactionEventProducer transactionEventProducer;

  public TransactionController(TransactionEventProducer transactionEventProducer) {
    this.transactionEventProducer = transactionEventProducer;
  }

  @PostMapping
  public ResponseEntity<Void> produceTransactionEvent(@RequestBody TransactionRequest request)
      throws ExecutionException, InterruptedException {

    TransactionEvent transactionEvent = TransactionEvent.newBuilder()
        .setFromName(request.fromName())
        .setFromIban(request.fromIban())
        .setToName(request.toName())
        .setToIban(request.toIban())
        .setAmount(request.amount())
        .setCurrency(Currency.valueOf(request.currency()))
        .setDescription(request.description())
        .setTransactionTimeMillis(Instant.now())
        .build();

    transactionEventProducer.produce(transactionEvent);

    return ResponseEntity.status(HttpStatus.CREATED).build();
  }

  public record TransactionRequest(
      String fromName,
      String fromIban,
      String toName,
      String toIban,
      BigDecimal amount,
      String currency,
      String description
  ) {}
}
