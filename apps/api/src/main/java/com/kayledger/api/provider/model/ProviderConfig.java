package com.kayledger.api.provider.model;

import java.time.Instant;
import java.util.UUID;

public record ProviderConfig(
        UUID id,
        UUID workspaceId,
        String providerKey,
        String displayName,
        String signingSecret,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
