package com.example.spring.kafka4.producer.generator;

import com.example.spring.kafka.avro.transactions.Currency;
import com.example.spring.kafka.avro.transactions.TransactionEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;

@Component
public class RandomTransactionEventGenerator {

    private static final Random random = new Random();
    private static final String[] FIRST_NAMES = {
            "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"
    };
    private static final String[] LAST_NAMES = {
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"
    };
    private static final String[] DESCRIPTIONS = {
            "Payment for services", "Salary transfer", "Bill payment", "Loan repayment",
            "Subscription fee", "Refund", "Dividend payment", "Investment", null, null
    };

    public TransactionEvent generate() {
        String fromName = generateName();
        String toName = generateName();
        String fromIban = generateIban();
        String toIban = generateIban();
        Currency currency = getRandomCurrency();
        BigDecimal amount = generateRandomAmount();
        String description = getRandomDescription();

        return TransactionEvent.newBuilder()
                .setFromName(fromName)
                .setFromIban(fromIban)
                .setToName(toName)
                .setToIban(toIban)
                .setCurrency(currency)
                .setAmount(amount)
                .setDescription(description)
                .setTransactionTimeMillis(Instant.now())
                .build();
    }

    private String generateName() {
        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
        return firstName + " " + lastName;
    }

    private String generateIban() {
        // Generate a simplified IBAN format: COUNTRYCODE + 2 check digits + 14 random digits
        String countryCode = "DE"; // Using Germany as default
        String checkDigits = String.format("%02d", random.nextInt(100));
        String bankCode = String.format("%05d", random.nextInt(100000));
        String accountNumber = String.format("%010d", random.nextInt(1000000000));
        return countryCode + checkDigits + bankCode + accountNumber;
    }

    private BigDecimal generateRandomAmount() {
        // Generate amount between 0.01 and 10000.00
        double randomAmount = 0.01 + (random.nextDouble() * 9999.99);
        return new BigDecimal(randomAmount).setScale(2, java.math.RoundingMode.HALF_UP);
    }

    private Currency getRandomCurrency() {
        Currency[] currencies = Currency.values();
        return currencies[random.nextInt(currencies.length)];
    }

    private String getRandomDescription() {
        return DESCRIPTIONS[random.nextInt(DESCRIPTIONS.length)];
    }
}
