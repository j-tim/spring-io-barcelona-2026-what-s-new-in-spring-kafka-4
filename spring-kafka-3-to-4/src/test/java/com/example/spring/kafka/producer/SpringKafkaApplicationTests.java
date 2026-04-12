package com.example.spring.kafka.producer;

import com.example.spring.kafka.event.TransactionEventJson;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        zookeeperPort = 2181,
        zkConnectionTimeout = 20000,
        zkSessionTimeout = 20000,
        kraft = false,
        partitions = 3
)
class SpringKafkaApplicationTests {

    @Autowired
    private KafkaTransactionEventJsonProducer producer;

    @Autowired
    private TransactionEventGenerator transactionEventGenerator;

    @Autowired
    private ConsumerFactory<String, TransactionEventJson> consumerFactory;

    private Consumer<String, TransactionEventJson> consumer;

    @Value("${topic.name}")
    private String topicName;

    @BeforeEach
    void setup() throws InterruptedException {
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(List.of(topicName));
    }

    @Test
    void shouldSendAndReceiveTransactionEvent() {
        TransactionEventJson expectedEvent = transactionEventGenerator.generate();
        producer.produce(expectedEvent);

        ConsumerRecords<String, TransactionEventJson> records =
                KafkaTestUtils.getRecords(consumer);

        assertThat(records.count()).isEqualTo(1);
        TransactionEventJson event = records.iterator().next().value();

        assertThat(event)
                .usingRecursiveComparison()
                .isEqualTo(expectedEvent);
    }
}
