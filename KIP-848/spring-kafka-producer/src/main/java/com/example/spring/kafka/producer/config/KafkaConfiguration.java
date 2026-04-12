package com.example.spring.kafka.producer.config;

import com.example.spring.kafka.avro.stock.quote.StockQuote;
import com.example.spring.kafka.producer.AvroStockQuoteProducer;
import com.example.spring.kafka.producer.ScheduledAvroStockQuoteProducer;
import com.example.spring.kafka.producer.generator.RandomStockQuoteGenerator;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaConfiguration {

  public static final String STOCK_QUOTES_TOPIC_NAME_AVRO = "stock-quotes";

  @Bean
  public NewTopic stockQuotesTopic() {
    return TopicBuilder.name(STOCK_QUOTES_TOPIC_NAME_AVRO)
        .partitions(3)
        .build();
  }

  @Bean
  @ConditionalOnProperty(name = "kafka.producer.enabled", havingValue = "true")
  RandomStockQuoteGenerator randomStockQuoteGenerator() {
    return new RandomStockQuoteGenerator();
  }

  @Bean
  AvroStockQuoteProducer avroStockQuoteProducer(KafkaTemplate<String, StockQuote> kafkaTemplate) {
    return new AvroStockQuoteProducer(kafkaTemplate);
  }

  @Bean
  @ConditionalOnProperty(name = "kafka.producer.enabled", havingValue = "true")
  ScheduledAvroStockQuoteProducer scheduledAvroStockQuoteProducer(
      AvroStockQuoteProducer avroStockQuoteProducer, RandomStockQuoteGenerator generator) {
    return new ScheduledAvroStockQuoteProducer(avroStockQuoteProducer, generator);
  }
}
