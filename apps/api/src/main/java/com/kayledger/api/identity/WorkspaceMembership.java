package com.kayledger.api.identity;

import java.time.Instant;
import java.util.UUID;

public record WorkspaceMembership(
        UUID id,
        UUID workspaceId,
        UUID actorId,
        String role,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
