package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user removes an item from their cart.
 */
public record RemoveFromCartEvent(
        String eventId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String productId,
        double productPrice,
        int quantity
) implements ClickEvent {
    public double lineItemTotal() {
        return productPrice * quantity;
    }
}