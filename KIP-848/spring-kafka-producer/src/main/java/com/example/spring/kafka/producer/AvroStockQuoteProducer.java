package com.example.spring.kafka.producer;

import com.example.spring.kafka.avro.stock.quote.StockQuote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.ExecutionException;

import static com.example.spring.kafka.producer.config.KafkaConfiguration.STOCK_QUOTES_TOPIC_NAME_AVRO;

public class AvroStockQuoteProducer {

  private static final Logger log = LoggerFactory.getLogger(AvroStockQuoteProducer.class);

  private final KafkaTemplate<String, StockQuote> kafkaTemplate;

  public AvroStockQuoteProducer(KafkaTemplate<String, StockQuote> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void produce(StockQuote stockQuote) throws ExecutionException, InterruptedException {
    SendResult<String, StockQuote> result = kafkaTemplate.send(
        STOCK_QUOTES_TOPIC_NAME_AVRO, stockQuote.getSymbol(), stockQuote).get();

    String topic = result.getRecordMetadata().topic();
    int partition = result.getRecordMetadata().partition();

    log.info("Produced to topic: [{}], partition: [{}] Data: {}", topic, partition, stockQuote);
  }

}
