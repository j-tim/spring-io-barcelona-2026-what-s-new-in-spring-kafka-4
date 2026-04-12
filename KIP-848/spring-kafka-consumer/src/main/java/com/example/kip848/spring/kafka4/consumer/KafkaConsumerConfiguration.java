package com.example.kip848.spring.kafka4.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaConsumerConfiguration {
    
    @Bean
    public NewTopic stockQuotesTopic() {
        return TopicBuilder.name("stock-quotes")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
