package com.example.spring.kafka.producer;

import com.example.spring.kafka.avro.stock.quote.StockQuote;
import com.example.spring.kafka.producer.generator.RandomStockQuoteGenerator;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.ExecutionException;

public class ScheduledAvroStockQuoteProducer {

  private final AvroStockQuoteProducer avroStockQuoteProducer;
  private final RandomStockQuoteGenerator generator;

  public ScheduledAvroStockQuoteProducer(AvroStockQuoteProducer avroStockQuoteProducer,
      RandomStockQuoteGenerator generator) {
    this.avroStockQuoteProducer = avroStockQuoteProducer;
    this.generator = generator;
  }

  @Scheduled(fixedRateString = "${kafka.producer.rate-ms}")
  public void produce() throws ExecutionException, InterruptedException {
    StockQuote stockQuote = generator.generate();
    avroStockQuoteProducer.produce(stockQuote);
  }
}
