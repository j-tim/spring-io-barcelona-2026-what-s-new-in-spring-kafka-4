package com.example.spring.kafka.producer.rest;

import com.example.spring.kafka.avro.stock.quote.StockQuote;
import com.example.spring.kafka.producer.AvroStockQuoteProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/stock-quotes")
public class StockQuoteController {

  private final AvroStockQuoteProducer avroStockQuoteProducer;

  public StockQuoteController(AvroStockQuoteProducer avroStockQuoteProducer) {
    this.avroStockQuoteProducer = avroStockQuoteProducer;
  }

  @PostMapping
  public ResponseEntity<Void> produceStockQuote(@RequestBody StockQuoteRequest request)
      throws ExecutionException, InterruptedException {
    StockQuote stockQuote = StockQuote.newBuilder()
        .setSymbol(request.symbol())
        .setExchange(request.exchange())
        .setTradeValue(request.tradeValue())
        .setCurrency(request.currency())
        .setDescription(request.description())
        .setTradeTime(Instant.now())
        .build();

    avroStockQuoteProducer.produce(stockQuote);

    return ResponseEntity.status(HttpStatus.CREATED).build();
  }

  public record StockQuoteRequest(
      String symbol,
      String exchange,
      BigDecimal tradeValue,
      String currency,
      String description
  ) {}
}
