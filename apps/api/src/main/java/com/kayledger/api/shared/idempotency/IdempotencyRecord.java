package com.kayledger.api.shared.idempotency;

import java.time.Instant;
import java.util.UUID;

public record IdempotencyRecord(
        UUID id,
        String scopeKind,
        UUID workspaceId,
        UUID actorId,
        String routeKey,
        String idempotencyKey,
        String requestHash,
        String status,
        Integer responseStatusCode,
        String responseBody,
        Instant createdAt,
        Instant updatedAt) {
}
