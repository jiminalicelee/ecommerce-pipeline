# Real-time e-commerce clickstream pipeline

A small event-driven data pipeline built in Java, demonstrating Kafka
and MongoDB working together: a producer simulates e-commerce
clickstream events (views, cart actions, purchases, searches),
publishes them to Kafka, and a consumer persists them into MongoDB.

## How it works

- **Producer** continuously generates random `ClickEvent`s (via
  `FakeEventGenerator`) and publishes each to the `clickstream-events`
  topic, keyed by `anonymousId`, until it receives a shutdown signal.
  Events are emitted at a jittered interval (200–800ms) to simulate
  realistic-ish traffic rather than a perfectly even beat. Values are
  serialized with `EventSerializer`; keys still use Kafka's built-in
  `StringSerializer`.
- **Consumer** subscribes to `clickstream-events`, deserializes each
  record's value into a `ClickEvent` with `EventDeserializer`, and
  dispatches on the concrete event type (exhaustive `switch` over the
  sealed interface) to `EventPersister`, which writes it into MongoDB.
  It also logs a short per-type description (`Product viewed: ...`,
  `Added to cart: ...`, etc.) alongside the record's
  key/partition/offset.
- **Persistence** (`EventPersister`, used by the consumer) routes each
  event type into its own MongoDB collection in the
  `ecommerce_analytics` database: `product_views`, `add_to_cart`,
  `remove_from_cart`, `searches`, and `purchases`. Each event is
  serialized back to JSON (via the same Jackson mapper used for Kafka)
  and inserted as a `Document`.
- **Event model** (`com.pipeline.model`) is defined: a sealed
  `ClickEvent` interface implemented by `ProductViewEvent`,
  `AddToCartEvent`, `RemoveFromCartEvent`, `PurchaseEvent`, and
  `SearchEvent` records. Each carries both `anonymousId` (always
  present) and `userId` (nullable, populated only post-login; see
  "Keying by `anonymousId`" below). Jackson (`jackson-databind` +
  `jackson-datatype-jsr310`, the latter needed for `Instant` fields)
  handles JSON serialization and deserialization, including polymorphism
  via `@JsonTypeInfo`/`@JsonSubTypes` on `ClickEvent` (an `eventType`
  property in the JSON picks the concrete record).
- **Serialization** (`com.pipeline.serialization`) provides
  `EventSerializer`/`EventDeserializer`, Kafka
  `Serializer<ClickEvent>`/`Deserializer<ClickEvent>` implementations,
  and `JsonMapperFactory`, which builds the single shared
  `ObjectMapper` configuration (registers `JavaTimeModule`, disables
  `WRITE_DATES_AS_TIMESTAMPS`) used by both the Kafka serde and
  `EventPersister`'s Mongo writes.

Requires JDK 21 for exhaustive `switch` pattern matching over the sealed
`ClickEvent` interface in the consumer.

## Design Decisions

### Why Kafka

Clickstream data is a natural fit for an event streaming platform:
it's high-volume, append-only, produced continuously by many
independent clients, and consumed by potentially multiple downstream
systems (in this project, just MongoDB persistence, but realistically
also could feed real-time analytics, fraud detection, or
recommendation systems without changing how events are produced).

Kafka specifically offers a few properties this project relies on
directly:

- **Decoupling producers from consumers.** The producer knows nothing
  about MongoDB; it just publishes events to a topic. This means
  additional consumers could be added later (e.g., a real-time
  dashboard) without touching the producer at all.
- **Ordering guarantees within a partition.** Keying records by
  `anonymousId` ensures all events from a given client are processed
  in the order they occurred, which matters for reconstructing
  accurate session behavior.
- **Durability and replay.** Events aren't lost if a consumer is down
  temporarily, and consumer offset tracking (per consumer group)
  allows reprocessing from a specific point if needed.

### Why MongoDB

Click events are heterogeneous by nature; a `ProductViewEvent` and a
`PurchaseEvent` share some fields but diverge significantly in shape
(e.g., `PurchaseEvent` contains a nested list of line items with no
equivalent in a `SearchEvent`). Modeling this in a relational schema
would mean either a sparse, nullable-heavy single table or a
normalized multi-table structure with joins for something that's
conceptually a single, self-contained event.

MongoDB's document model maps naturally onto this: each event type is
persisted as a document whose shape matches its own record definition
exactly, with no schema migration required to accommodate new event
types or fields. Nested structures like `PurchaseEvent`'s line items
are stored as embedded arrays rather than requiring a join, which
matches how the data is actually accessed; a purchase and its line
items are always read together, never independently.

### Sealed Interface for Event Modeling

`ClickEvent` is modeled as a `sealed interface` implemented by five record
types (`ProductViewEvent`, `AddToCartEvent`, `RemoveFromCartEvent`,
`SearchEvent`, `PurchaseEvent`), rather than a single generic event
envelope with a type discriminator field.

**Why:** Because the interface is sealed, the compiler knows the complete
set of possible event types at compile time. This enables exhaustive
pattern matching wherever event type is branched on, with no default case
needed. This is the actual dispatch in `EventConsumer`, routing each
event to its `EventPersister` handler:

```java
switch (event) {
    case ProductViewEvent e -> EventPersister.handleProductView(e, productViewsCollection);
    case AddToCartEvent e -> EventPersister.handleAddToCart(e, addToCartCollection);
    case RemoveFromCartEvent e -> EventPersister.handleRemoveFromCart(e, removeFromCartCollection);
    case SearchEvent e -> EventPersister.handleSearch(e, searchCollection);
    case PurchaseEvent e -> EventPersister.handlePurchase(e, purchaseCollection);
}
```

If a sixth event type is added later, any exhaustive switch over
`ClickEvent` won't compile until a matching case is added. A bug that
would otherwise surface silently at runtime (a new event type falling
through unhandled) becomes a compile-time error instead: leave a new
event type off this switch and the build fails, instead of the event
silently never getting written to Mongo.

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

### One MongoDB collection per event type

`EventPersister` writes each event type into its own collection
(`product_views`, `add_to_cart`, `remove_from_cart`, `searches`,
`purchases`) instead of a single polymorphic `events` collection.

**Why:** Each event type has a distinct shape (line items for
purchases, product/price for views and cart actions, a query string
for searches). Separate collections mean downstream queries and
aggregations for a given event type don't need to filter out the other
shapes first, at the cost of needing a fan-in (or `$unionWith`) if you
ever want a single unified event stream from Mongo directly.

## Directory Structure

```
.
├── docker-compose.yml              # Local Kafka + MongoDB containers
├── pom.xml                         # Maven build, dependencies, shade plugin
└── src/main/java/com/pipeline/
    ├── MongoConnectionCheck.java   # Dev utility: verify Mongo connectivity
    ├── RoundTripCheck.java         # Dev utility: verify Jackson serde round-trip
    ├── consumer/
    │   ├── EventConsumer.java      # Kafka consumer: poll, deserialize, dispatch
    │   └── EventPersister.java     # Per-event-type MongoDB writes
    ├── model/
    │   ├── ClickEvent.java         # Sealed interface for all event types
    │   ├── ProductViewEvent.java
    │   ├── AddToCartEvent.java
    │   ├── RemoveFromCartEvent.java
    │   ├── PurchaseEvent.java
    │   ├── SearchEvent.java
    │   └── OrderLineItem.java      # Embedded in PurchaseEvent
    ├── producer/
    │   ├── EventProducer.java      # Kafka producer: publish loop, shutdown hook
    │   └── FakeEventGenerator.java # Generates random ClickEvents
    └── serialization/
        ├── EventSerializer.java    # Kafka Serializer<ClickEvent>
        ├── EventDeserializer.java  # Kafka Deserializer<ClickEvent>
        └── JsonMapperFactory.java  # Shared Jackson ObjectMapper config
```

## Running locally

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

You should see the producer log a line every ~200-800ms like `Sent
ProductViewEvent to partition 0 at offset 0`, and the consumer log a
matching line like `Product viewed: ProductViewEvent[...] (Key: ...,
Partition: 0, Offset: 0)`. (If you have multiple consumer instances
running at once, they share `clickstream-events`'s partitions as one
consumer group, so a given message only shows up in whichever instance
owns that partition.)

Each logged event is also written to MongoDB. To confirm documents are
landing, connect with `mongosh` (or any Mongo client) and check the
`ecommerce_analytics` database:

```bash
mongosh mongodb://localhost:27017/ecommerce_analytics --eval "db.product_views.find().limit(1)"
```

### Development utilities

Two standalone `main` classes at the top level are ad hoc checks used
while wiring up MongoDB and Jackson, not part of the running pipeline:

- `MongoConnectionCheck`: inserts and reads back a throwaway document
  to confirm the Java driver can reach the local Mongo container.
- `RoundTripCheck`: serializes a sample `AddToCartEvent` with
  `EventSerializer` and deserializes it back with `EventDeserializer`,
  asserting every field (including the nullable `userId`) survives the
  round trip.

Run either with `mvn exec:java -Dexec.mainClass="com.pipeline.MongoConnectionCheck"`
(or `...RoundTripCheck`).
