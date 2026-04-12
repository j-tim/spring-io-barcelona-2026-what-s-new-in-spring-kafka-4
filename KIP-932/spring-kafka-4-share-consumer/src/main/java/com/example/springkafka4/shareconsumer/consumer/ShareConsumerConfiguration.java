package com.example.springkafka4.shareconsumer.consumer;

import com.example.spring.kafka.avro.transactions.TransactionEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;

@Configuration
@EnableKafka
public class ShareConsumerConfiguration {

  private final Map<String, Object> properties = new HashMap<>();

  // Current limitation no Spring Boot Auto Configuration for Spring Kafka Share consumer
  public ShareConsumerConfiguration() {
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

    properties.put("schema.registry.url", "http://localhost:8081");
    properties.put("specific.avro.reader", true);
  }

  /**
   * Factory that creates share consumers {@link org.apache.kafka.clients.consumer.ShareConsumer}
   */
  @Bean
  public ShareConsumerFactory<String, TransactionEvent> shareConsumerFactory() {
    return new DefaultShareConsumerFactory<>(properties);
  }
}