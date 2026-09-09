package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user views a product page.
 */
public record ProductViewEvent(
        String eventId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String productId,
        String productCategory,
        double productPrice
) implements ClickEvent {}