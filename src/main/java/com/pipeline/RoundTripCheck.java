package com.pipeline;

import java.time.Instant;
import java.util.UUID;

import com.pipeline.model.AddToCartEvent;
import com.pipeline.model.ClickEvent;
import com.pipeline.serialization.EventDeserializer;
import com.pipeline.serialization.EventSerializer;

public class RoundTripCheck {
    private static final String TOPIC = "clickstream-events";

    public static void main(String[] args) {
        try (EventSerializer serializer = new EventSerializer()) {
            try (EventDeserializer deserializer = new EventDeserializer()) {
                // Build a sample event
                String eventId = UUID.randomUUID().toString();
                String anonymousId = UUID.randomUUID().toString();
                String sessionId = UUID.randomUUID().toString();
                Instant originalTimestamp = Instant.now();
                String productId = UUID.randomUUID().toString();
                double productPrice = 25.00;
                int quantity = 2;

                AddToCartEvent original = new AddToCartEvent(
                    eventId,
                    anonymousId,
                    null,             // userId - simulating pre-login
                    sessionId,
                    originalTimestamp,
                    productId,
                    productPrice,
                    quantity
                );

                // Serialize
                byte[] serializedBytes = serializer.serialize(TOPIC, original);
                String json = new String(serializedBytes);
                System.out.println("Serialized JSON: " + json);

                // Deserialize
                ClickEvent deserialized = deserializer.deserialize(TOPIC, serializedBytes);
                System.out.println("Deserialized type: " + deserialized.getClass().getSimpleName());

                // Verify round trip
                if (deserialized instanceof AddToCartEvent d) {
                    boolean eventIdMatch = eventId.equals(d.eventId());
                    boolean anonymousIdMatch = anonymousId.equals(d.anonymousId());
                    boolean userIdMatch = d.userId() == null;
                    boolean sessionIdMatch = sessionId.equals(d.sessionId());
                    boolean timestampMatch = originalTimestamp.equals(d.eventTimestamp());
                    boolean productIdMatch = productId.equals(d.productId());
                    boolean priceMatch = productPrice == d.productPrice();
                    boolean quantityMatch = quantity == d.quantity();

                    System.out.println("eventId match: " + eventIdMatch);
                    System.out.println("anonymousId match: " + anonymousIdMatch);
                    System.out.println("userId match (null): " + userIdMatch);
                    System.out.println("sessionId match: " + sessionIdMatch);
                    System.out.println("timestamp match: " + timestampMatch
                        + " (original=" + originalTimestamp + ", deserialized=" + d.eventTimestamp() + ")");
                    System.out.println("productId match: " + productIdMatch);
                    System.out.println("price match: " + priceMatch);
                    System.out.println("quantity match: " + quantityMatch);

                    boolean allMatch = eventIdMatch && anonymousIdMatch && userIdMatch
                        && sessionIdMatch && timestampMatch && productIdMatch
                        && priceMatch && quantityMatch;

                    System.out.println(allMatch ? "ROUND TRIP PASSED" : "ROUND TRIP FAILED");
                } else {
                    System.out.println("FAILED: deserialized object is not an AddToCartEvent");
                }
            }
        }
    }
}