package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user adds an item to their cart.
 * AddToCartEvent
 * @param eventId
 * @param userId
 * @param anonymousId
 * @param sessionId
 * @param eventTimestamp
 * @param productId
 * @param productPrice
 * @param quantity
 */
public record AddToCartEvent(
        String eventId,
        String userId,
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