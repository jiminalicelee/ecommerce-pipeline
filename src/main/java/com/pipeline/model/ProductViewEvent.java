package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user views a product page.
 * ProductViewEvent
 * @param eventId
 * @param userId
 * @param anonymousId
 * @param sessionId
 * @param eventTimestamp
 * @param productId
 * @param productCategory
 * @param productPrice
 */
public record ProductViewEvent(
        String eventId,
        String userId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String productId,
        String productCategory,
        double productPrice
) implements ClickEvent {}