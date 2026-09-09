package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user adds an item to their cart.
 */
public record AddToCartEvent(
        String eventId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String productId,
        double productPrice,
        int quantity) implements ClickEvent {
    public double lineItemTotal() {
        return productPrice * quantity;
    }
}