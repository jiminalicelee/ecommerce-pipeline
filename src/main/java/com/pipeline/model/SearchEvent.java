package com.pipeline.model;

import java.time.Instant;

/**
 * Fired when a user searches for an item.
 */
public record SearchEvent(
        String eventId,
        String anonymousId,
        String sessionId,
        Instant eventTimestamp,
        String searchQuery,
        int resultCount
) implements ClickEvent {}