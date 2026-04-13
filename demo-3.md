# Demo 3: KIP-848: The Next Generation of the Consumer Rebalance Protocol

## Index

1. [Prepare before the demo](#prepare-before-the-demo)
    - [Build the project](#build-the-project)
    - [Build OCI Images](#build-oci-images)
2. [Start demo](#start-demo)
    - [Start infrastructure](#start-infrastructure)
    - [Observe the consumer group](#observe-the-consumer-group)
    - [Start live upgrade (new consumer protocol)](#start-live-upgrade-new-consumer-protocol)
    - [Opt in on the new consumer protocol for one instance](#opt-in-on-the-new-consumer-protocol-for-one-instance)
    - [Scale down the consumers running with the classic protocol](#scale-down-the-consumers-running-with-the-classic-protocol)
    - [Rollback supported?](#rollback-supported)
    - [Shutdown the infrastructure](#shutdown-the-infrastructure)
3. [Takeaways & Trade-offs](#takeaways--trade-offs)

## Prepare before the demo

### Build the project

```bash
./mvnw clean install
```

### Build OCI Images

Make sure to login to Docker Hub before building the image:

```bash
docker login
```

```bash
cd KIP-848
./buildImages.sh
```

This creates an OCI-compliant image named: 
* `spring-kafka-producer:0.0.1-SNAPSHOT`
* `spring-kafka-consumer:0.0.1-SNAPSHOT`

```bash
docker images | grep -E "kafka-producer|kafka-consumer"
```

## Start demo

### Start infrastructure

Start the:

* Kafka cluster 
* 1 producer instance
* 3 consumer instances (running with the classic consumer protocol) in the consumer group `my-consumer-group`.

```bash
docker compose down -v
docker compose -f docker-compose-applications.yml -p demo up -d
```

### Observe the consumer group

List all consumer groups including the type of consumer protocol they are using:

```bash
watch docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
--list \
--type
```

Show members of the consumer group `my-consumer-group`

```bash
watch docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
--describe \
--group my-consumer-group \
--members
```

Show state of the consumer group `my-consumer-group`

```bash
watch docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
--describe \
--group my-consumer-group \
--state
```

### Start live upgrade (new consumer protocol):

Let's start the live upgrade. By starting a new instance:
* in the consumer group: `my-consumer-group` 
* using `group.protocol = consumer` (configured using Spring Kafka consumer property `spring.kafka.consumer.properties.group.protocol=consumer`)

[docker-compose-applications.yml](docker-compose-applications.yml)

### Opt in on the new consumer protocol for one instance:

Scale from 0 to 1 instance: 

```bash
docker compose -f docker-compose-applications.yml -p demo scale consumer-new-consumer-protocol=1
```

```bash
docker compose logs -f kafka
```

Observe the logs of the Kafka broker:
The consumer group is converted from a `classic` group into a `consumer` group.

```log
[2026-03-31 08:44:37,138] INFO [GroupCoordinator id=1 topic=__consumer_offsets partition=13] [GroupId my-consumer-group] Converted the classic group to a consumer group. (org.apache.kafka.coordinator.group.GroupMetadataManager)
```

Scale from 1 to 3 instance

```bash
docker compose -f docker-compose-applications.yml -p demo scale consumer-new-consumer-protocol=3
```
### Scale down the consumers running with the classic protocol

```bash
docker compose -f docker-compose-applications.yml -p demo scale consumer-classic-protocol=2
```

> Complete the migration. Don't leave the group in a mixed state for extended periods.

```bash
docker compose -f docker-compose-applications.yml -p demo scale consumer-classic-protocol=0
```

All the consumer in the consumer group are now running with the new consumer protocol.


### Rollback supported?

What if issues arise after the migration and your Spring Kafka consumer application is using the new consumer protocol?
Just remove `group.protocol=consumer` from your configuration and restart consumers. 
The coordinator will automatically convert back to `classic` protocol when the last new-protocol consumer leaves.

### Shutdown the infrastructure

```bash
docker compose -f docker-compose-applications.yml -p demo down -v
```

## Takeaways & Trade-offs

The new protocol in Kafka 4 shifts rebalance logic from the client to the broker-side group coordinator.

* Assignment strategy
   * now controlled by the broker
   * not by the client

| Area                  | Benefit                                                                                   | Trade‑off                                                                                                             |
|-----------------------|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| **Rebalance speed**   | Much faster, incremental rebalances without global synchronization barriers               | Behavior differs from classic eager/cooperative semantics; applications relying on old callbacks may need adjustments |
| **Scalability**       | More reliable and stable operation in large consumer groups                               | More coordination load is shifted to brokers instead of clients                                                       |
| **Client complexity** | Simpler consumers                                                                         | Monitoring and observability must adapt due to new metrics and threading model changes                                |
| **Config/API**        | Cleaner design overall with fewer client‑side tuning knobs needed                         | Several existing configs and APIs are deprecated (e.g., heartbeat/session configs, enforceRebalance APIs)             |
| **Operational model** | Server-side control improves consistency                                                  | Less client-side assignor flexibility                                                                                 |

The new protocol:

* New consumer metrics Kafka client metrics:
  * [group.protocol=classic](KIP-848/metrics/micrometer-kafka-metrics-classic-consumer-group.json)
  * [group.protocol=consumer](KIP-848/metrics/micrometer-kafka-metrics-new-consumer-group.json)
* Disables several classic consumer configurations, such as:
  * `partition.assignment.strategy`
  * `heartbeat.interval.ms`
  * `session.timeout.ms`
* Custom assignment strategies are not supported with the new consumer protocol, as the assignment logic is now fully controlled by the broker.