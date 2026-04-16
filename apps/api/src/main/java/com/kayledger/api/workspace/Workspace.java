package com.kayledger.api.workspace;

import java.time.Instant;
import java.util.UUID;

public record Workspace(
        UUID id,
        String slug,
        String displayName,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
