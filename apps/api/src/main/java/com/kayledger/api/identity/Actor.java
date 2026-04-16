package com.kayledger.api.identity;

import java.time.Instant;
import java.util.UUID;

public record Actor(
        UUID id,
        String actorKey,
        String displayName,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
