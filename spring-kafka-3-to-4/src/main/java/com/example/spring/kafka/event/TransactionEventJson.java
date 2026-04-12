package com.example.spring.kafka.event;

import java.math.BigDecimal;
import java.time.Instant;

public record TransactionEventJson(
        String fromName,
        String fromIban,
        String toName,
        String toIban,
        CurrencyJson currency,
        BigDecimal amount,
        String description,
        Instant transactionTimestamp
) {
}
