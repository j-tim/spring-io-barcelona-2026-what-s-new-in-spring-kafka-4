# Demo 1: A practical migration path to Spring Boot 4 and Spring Kafka 4

## Run the application with Spring Boot 3 and Spring Kafka 3

```bash
./mvnw clean install 
```

```bash
docker compose up -d
```

Start the producer and the consumer:

```bash
./mvnw spring-boot:run -pl spring-kafka-3-to-4
```

## Update to Spring Boot 4 and Spring Kafka 4 using OpenRewrite

To actually apply the migration changes to your code:

```bash
cd spring-kafka-3-to-4
./mvnw rewrite:run 
```

* This will:
   - Upgrade Spring Boot dependencies from 3.x to 4.x
   - Upgrade Spring Kafka dependencies from 3.x to 4.x (new Spring Kafka starters)
   - Upgrade to Jackson 3: 
     - including Spring Boot Jackson starter
     - configuration changes for the serializer and deserializer
     - new Jackson 3 package names
   - Rely on the new KRaft mode in Kafka 4.x which eliminates the need for Zookeeper.
     - Remove the Zookeeper configuration properties on Spring Kafka @EmbeddedKafka annotation. 

### Verify the Update

After running the migration:

1. Review the changes made by OpenRewrite
2. Rebuild the project and run the test
3. Review and update any manual changes that may be required

```bash
cd spring-kafka-3-to-4
./mvnw clean install 
```

## Run the application with Spring Boot 4 and Spring Kafka 4

```bash
./mvnw spring-boot:run -pl spring-kafka-3-to-4
```
