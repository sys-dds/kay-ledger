package com.kayledger.api.identity;

import java.time.Instant;
import java.util.UUID;

public record ProviderProfile(
        UUID id,
        UUID workspaceId,
        UUID actorId,
        String displayName,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
