package com.example.spring.kafka.producer;

import com.example.spring.kafka.event.CurrencyJson;
import com.example.spring.kafka.event.TransactionEventJson;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

@Component
public class TransactionEventGenerator {

    public TransactionEventJson generate() {
        String fromName = "John Do";
        String fromIban = "NL80INGB6771015777";
        String toName = "Jane Do";
        String toIban = "NL80INGB6771015778";
        BigDecimal amount = new BigDecimal("1000.99");
        String description = "Some description";
        Instant now = Instant.now();
        return new TransactionEventJson(fromName, fromIban, toName, toIban, CurrencyJson.EUR, amount, description, now);
    }
}
