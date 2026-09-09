package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user searches for an item.
 * SearchEvent
 * @param eventId
 * @param userId
 * @param anonymousId
 * @param sessionId
 * @param eventTimestamp
 * @param searchQuery
 * @param resultCount
 */
public record SearchEvent(
        String eventId,
        String userId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String searchQuery,
        int resultCount
) implements ClickEvent {}