# Demo 2: KIP-932: Queues for Kafka (Share Consumers)

## Start the Kafka cluster:

```shell
docker compose up -d
```

## Show the consumer application

* [pom.xml](KIP-932/spring-kafka-4-share-consumer/pom.xml)
* [application.yaml](KIP-932/spring-kafka-4-share-consumer/src/main/resources/application.yaml)
* [ShareConsumerConfiguration.java](KIP-932/spring-kafka-4-share-consumer/src/main/java/com/example/springkafka4/shareconsumer/consumer/ShareConsumerConfiguration.java)
* [ManualShareConsumerConfiguration.java](KIP-932/spring-kafka-4-share-consumer/src/main/java/com/example/springkafka4/shareconsumer/consumer/manual/ManualShareConsumerConfiguration.java)
* [AnnotationDrivenManualKafkaShareConsumer.java](KIP-932/spring-kafka-4-share-consumer/src/main/java/com/example/springkafka4/shareconsumer/consumer/manual/annotation/AnnotationDrivenManualKafkaShareConsumer.java)

## Acknowledgment Mode Comparison

| 	Mode                | Who acknowledges | On success                     | On listener error                        | When to use?                                                                                      |
|----------------------|------------------|--------------------------------|------------------------------------------|---------------------------------------------------------------------------------------------------|
| EXPLICIT (default)   | Container        | Container sends `ACCEPT`       | Recoverer decides (`REJECT` by default)  | your business logic doesn't require fine grained control over the acknowledgment outcome          |
| MANUAL               | Listener code    | Listener calls `acknowledge()` | Listener calls `release()` or `reject()` | your business logic require fine grained control over the acknowledgment outcome record by record |
| IMPLICIT             | Kafka broker     | Broker auto-ACCEPTs            | Broker auto-ACCEPTs (no recovery)        | Use this mode only when per-record delivery guarantees are not required                           |                                                                           |

## Spring Profiles

| Profile    | Acknowledgment Mode | Description                                                                                       |
|------------|---------------------|---------------------------------------------------------------------------------------------------|
| `explicit` | Explicit            | Configuration bean profile that sets up the explicit share acknowledgment mode container factory. |
| `implicit` | Implicit            | Configuration bean profile that sets up the implicit share acknowledgment mode container factory. |
| `manual`   | Manual              | Configuration bean profile that sets up the manual share acknowledgment mode container factory.   |

| Profile                     | Type of consumer  | Acknowledgment | Description                             |
|-----------------------------|-------------------|----------------|-----------------------------------------|
| `annotation`                | Annotation-driven | Implicit       | Uses @KafkaListener                     |
| `programmatic`              | Programmatic      | Explicit       | Uses ShareKafkaMessageListenerContainer |


## Start the producer application

```bash
cd KIP-932/spring-kafka-4-producer
./mvnw spring-boot:run
```

## Start the consumer application

To run with a specific profile(s):

```bash
cd KIP-932/spring-kafka-4-share-consumer
./mvnw spring-boot:run -Dspring-boot.run.profiles=annotation, manual
```

## Show happy path and error scenarios for Share Consumers with explicit acknowledgment

[requests.http](KIP-932/spring-kafka-4-producer/requests.http)

[Show AnnotationDrivenManualKafkaShareConsumer.java](KIP-932/spring-kafka-4-share-consumer/src/main/java/com/example/springkafka4/shareconsumer/consumer/manual/annotation/AnnotationDrivenManualKafkaShareConsumer.java)

## Show the state of the share group:

List all share groups:

> kafka-share-groups --bootstrap-server <broker>:<port> --list

```bash
docker exec -it kafka kafka-share-groups --bootstrap-server kafka:9092 --list
```

Describe a specific share group:

> kafka-share-groups --bootstrap-server <broker>:<port> --describe --group <group-name>

```bash
docker exec -it kafka kafka-share-groups --bootstrap-server kafka:29092 --describe --group annotation-driven-implicit-share-group
```

## Read share group state internal topic

```bash
docker exec -it kafka \
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --formatter-property print.key=true \
  --topic __share_group_state \
  --from-beginning \
  --formatter=org.apache.kafka.tools.consumer.group.share.ShareGroupStateMessageFormatter
```

## Stop the Kafka cluster:

```shell
docker compose down -v
```
