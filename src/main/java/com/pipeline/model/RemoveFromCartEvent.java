package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user removes an item from their cart.
 * RemoveFromCartEvent
 * @param eventId
 * @param userId
 * @param anonymousId
 * @param sessionId
 * @param eventTimestamp
 * @param productId
 * @param productPrice
 * @param quantity
 */
public record RemoveFromCartEvent(
        String eventId,
        String userId,
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