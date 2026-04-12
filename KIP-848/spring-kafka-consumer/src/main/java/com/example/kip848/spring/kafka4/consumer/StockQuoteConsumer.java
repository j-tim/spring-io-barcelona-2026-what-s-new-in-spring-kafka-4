package com.example.kip848.spring.kafka4.consumer;

import com.example.spring.kafka.avro.stock.quote.StockQuote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class StockQuoteConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockQuoteConsumer.class);

    @Value("${spring.kafka.consumer.properties.group.protocol}")
    private String groupProtocol;

    @KafkaListener(topics = "stock-quotes")
    public void onEvent(StockQuote stockQuote, 
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Group protocol: [{}] Processing event from topic: [{}] partition: [{}] value: {}",
                 groupProtocol, "stock-quotes", partition, stockQuote);
    }
}
