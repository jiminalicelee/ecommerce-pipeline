package com.pipeline.model;

import java.time.Instant;

/**
 * Sealed interface — the closed set of event types allowed in this pipeline.
 * Add a new "permits" entry when adding a new record.
 */
public sealed interface ClickEvent
        permits ProductViewEvent, AddToCartEvent, RemoveFromCartEvent, PurchaseEvent, SearchEvent {

    // Common accessor every event type must provide.
    String eventId();
    String anonymousId(); // A random identifier (like a UUID) generated and stored
                          // on the client-side the first time any user shows up,
                          // logged in or not.
    String sessionId();
    Instant eventTimestamp();
}