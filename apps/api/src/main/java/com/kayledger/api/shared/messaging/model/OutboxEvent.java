package com.kayledger.api.shared.messaging.model;

import java.time.Instant;
import java.util.UUID;

public record OutboxEvent(
        UUID id,
        UUID workspaceId,
        String aggregateType,
        UUID aggregateId,
        String eventType,
        String payloadJson,
        String dedupeKey,
        String status,
        Instant availableAt,
        Instant publishedAt,
        int retryCount,
        String lastError,
        Instant parkedAt,
        Instant createdAt,
        Instant updatedAt) {
}
