package com.pipeline.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface — the closed set of event types allowed in this pipeline.
 * Add a new "permits" entry when adding a new record.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ProductViewEvent.class, name = "PRODUCT_VIEW"),
    @JsonSubTypes.Type(value = AddToCartEvent.class, name = "ADD_TO_CART"),
    @JsonSubTypes.Type(value = RemoveFromCartEvent.class, name = "REMOVE_FROM_CART"),
    @JsonSubTypes.Type(value = PurchaseEvent.class, name = "PURCHASE"),
    @JsonSubTypes.Type(value = SearchEvent.class, name = "SEARCH")
})
public sealed interface ClickEvent
        permits ProductViewEvent, AddToCartEvent, RemoveFromCartEvent, PurchaseEvent, SearchEvent {

    // Common accessor every event type must provide.
    String eventId();
    String anonymousId(); // A random identifier (like a UUID) generated and stored
                          // on the client-side the first time any user shows up,
                          // regardless of whether they are logged in or not.
    String sessionId();
    Instant eventTimestamp();
}