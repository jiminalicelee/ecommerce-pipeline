package com.pipeline.model;

import java.time.Instant;
import java.util.List;

/**
 * Fired when a user purchases items in their cart.
 */
public record PurchaseEvent(
        String eventId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        List<OrderLineItem> lineItems) implements ClickEvent {
    // Compact constructor: replace lineItems with a defensive copy
    // to prevent external mutation after construction.
    public PurchaseEvent {
        lineItems = List.copyOf(lineItems);
    }

    public double totalPrice() {
        return lineItems.stream()
                .mapToDouble(OrderLineItem::lineItemTotal)
                .sum();
    }
}