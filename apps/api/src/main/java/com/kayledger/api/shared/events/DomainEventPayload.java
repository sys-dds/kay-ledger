package com.kayledger.api.shared.events;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record DomainEventPayload(
        UUID eventId,
        UUID workspaceId,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        String dedupeKey,
        Instant occurredAt,
        Map<String, Object> data) {
}
