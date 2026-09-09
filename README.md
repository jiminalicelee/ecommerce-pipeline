# Real-time e-commerce clickstream pipeline

A small event-driven data pipeline built in Java, demonstrating Kafka
and MongoDB working together: a producer simulates e-commerce
clickstream events (views, cart actions, purchases, searches),
publishes them to Kafka, and a consumer processes them into MongoDB.

## Current status

This is early scaffolding, not the full pipeline described above yet:

- **Producer** sends a single hardcoded `"hello world"` record to the
  `clickstream-events` topic, then exits. It does not yet publish the
  event model below.
- **Consumer** subscribes to `clickstream-events` and logs each
  record's key/value/partition/offset to the console. It does not
  write to MongoDB yet; the Mongo container in `docker-compose.yml`
  is started but unused.
- **Event model** (`com.pipeline.model`) is defined: a sealed
  `ClickEvent` interface implemented by `ProductViewEvent`,
  `AddToCartEvent`, `RemoveFromCartEvent`, `PurchaseEvent`, and
  `SearchEvent` records. Jackson (`jackson-databind` +
  `jackson-datatype-jsr310`, the latter needed for `Instant` fields)
  handles JSON (de)serialization, including polymorphism via
  `@JsonTypeInfo`/`@JsonSubTypes` on `ClickEvent` (an `eventType`
  property in the JSON picks the concrete record).
- **Serialization** (`com.pipeline.serialization`) adds
  `EventSerializer`/`EventDeserializer`, Kafka
  `Serializer<ClickEvent>`/`Deserializer<ClickEvent>` implementations
  backed by the Jackson `ObjectMapper` above. They aren't wired into
  the producer/consumer yet — both still use Kafka's built-in
  `StringSerializer`/`StringDeserializer` for the `"hello world"`
  record described below.

## Running locally

Two ways to run the producer/consumer, depending on whether you want
a quick dev loop or a standalone jar.

### Option A: `mvn exec:java` (fastest for local iteration)

Resolves dependencies from Maven at run time — no packaging step, but
requires Maven installed.

```bash
# 1. Start Kafka + Mongo
docker compose up -d

# 2. Compile
mvn compile

# 3. In one terminal, start the consumer
mvn exec:java -Dexec.mainClass="com.pipeline.consumer.EventConsumer"

# 4. In another terminal, run the producer
mvn exec:java -Dexec.mainClass="com.pipeline.producer.EventProducer"
```

### Option B: runnable jar (no Maven needed at run time)

Bundles all dependencies into a single jar via `maven-shade-plugin`,
so it can run with just a JVM — e.g. in a Docker container.

```bash
# 1. Start Kafka and MongoDB
docker compose up -d

# 2. Build a runnable jar (all dependencies bundled via maven-shade-plugin)
mvn clean package

# 3. In one terminal, start the consumer
java -cp target/ecommerce-pipeline-1.0-SNAPSHOT.jar com.pipeline.consumer.EventConsumer

# 4. In another terminal, start the producer
java -cp target/ecommerce-pipeline-1.0-SNAPSHOT.jar com.pipeline.producer.EventProducer
```

---

Either way, you should see the producer log a line like `Sent to
partition 0 at offset 0`, and the consumer log the matching `Key:
null, Value: hello world` / `Partition: ..., Offset: ...` pair. (If
you have multiple consumer instances running at once, they share
`clickstream-events`'s partitions as one consumer group, so a given
message only shows up in whichever instance owns that partition.)