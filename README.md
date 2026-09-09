# Real-time e-commerce clickstream pipeline

A small event-driven data pipeline built in Java, demonstrating Kafka
and MongoDB working together: a producer simulates e-commerce
clickstream events (views, cart actions, purchases, searches),
publishes them to Kafka, and a consumer processes them into MongoDB.

## Current status

This is a work in progress, not the full pipeline described above yet:

- **Producer** continuously generates random `ClickEvent`s (via
  `FakeEventGenerator`, one every 500ms) and publishes each to the
  `clickstream-events` topic, keyed by `anonymousId`, until it
  receives a shutdown signal. Values are serialized with
  `EventSerializer`; keys still use Kafka's built-in
  `StringSerializer`.
- **Consumer** subscribes to `clickstream-events`, deserializes each
  record's value into a `ClickEvent` with `EventDeserializer`, and
  dispatches on the concrete event type (exhaustive `switch` over the
  sealed interface) to build a short per-type description
  (`Product viewed: ...`, `Added to cart: ...`, etc.), logged in a
  single line alongside the record's key/partition/offset. It does not
  write to MongoDB yet; the Mongo container in `docker-compose.yml` is
  started but unused.
- **Event model** (`com.pipeline.model`) is defined: a sealed
  `ClickEvent` interface implemented by `ProductViewEvent`,
  `AddToCartEvent`, `RemoveFromCartEvent`, `PurchaseEvent`, and
  `SearchEvent` records. Each carries both `anonymousId` (always
  present) and `userId` (nullable, populated only post-login — see
  "Keying by `anonymousId`" below). Jackson (`jackson-databind` +
  `jackson-datatype-jsr310`, the latter needed for `Instant` fields)
  handles JSON serialization and deserialization, including polymorphism
  via `@JsonTypeInfo`/`@JsonSubTypes` on `ClickEvent` (an `eventType`
  property in the JSON picks the concrete record).
- **Serialization** (`com.pipeline.serialization`) adds
  `EventSerializer`/`EventDeserializer`, Kafka
  `Serializer<ClickEvent>`/`Deserializer<ClickEvent>` implementations
  backed by the Jackson `ObjectMapper` above, and is now wired into
  both the producer and consumer.

Requires JDK 21 for exhaustive `switch` pattern matching over the sealed
`ClickEvent` interface in the consumer.

## Design Decisions

### Sealed Interface for Event Modeling

`ClickEvent` is modeled as a `sealed interface` implemented by five record
types (`ProductViewEvent`, `AddToCartEvent`, `RemoveFromCartEvent`,
`SearchEvent`, `PurchaseEvent`), rather than a single generic event
envelope with a type discriminator field.

**Why:** Because the interface is sealed, the compiler knows the complete
set of possible event types at compile time. This enables exhaustive
pattern matching wherever event type is branched on, with no default case
needed:

```java
String description = switch (event) {
    case ProductViewEvent e -> "Product viewed: " + e;
    case AddToCartEvent e -> "Added to cart: " + e;
    case RemoveFromCartEvent e -> "Removed from cart: " + e;
    case SearchEvent e -> "Search performed: " + e;
    case PurchaseEvent e -> "Purchase completed: " + e;
};
```

If a sixth event type is added later, any exhaustive switch over
`ClickEvent` won't compile until a matching case is added. A bug that
would otherwise surface silently at runtime (a new event type falling
through unhandled) becomes a compile-time error instead.

### Keying by `anonymousId`

Producer records are keyed by `anonymousId` for every event, regardless of
whether `userId` is populated.

**Why:** `anonymousId` is assigned once per client and persists across the
login boundary, while `userId` only exists post-authentication. Keying
consistently by `anonymousId` ensures all events from a given client
(before _and_ after login) land on the same partition, preserving
per-client ordering within a session. Keying by `userId` when present
would switch a user's events to a different partition mid-session the
moment they log in, breaking that ordering guarantee. `userId` remains
available in the event payload for downstream identity resolution but
never determines partition placement.

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

Either way, you should see the producer log a line every ~500ms like
`Sent ProductViewEvent to partition 0 at offset 0`, and the consumer
log a matching line like `Product viewed: ProductViewEvent[...] (Key:
..., Partition: 0, Offset: 0)`. (If you have multiple consumer
instances running at once, they share `clickstream-events`'s
partitions as one consumer group, so a given message only shows up in
whichever instance owns that partition.)