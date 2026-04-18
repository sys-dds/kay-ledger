package com.kayledger.api.provider.model;

import java.time.Instant;
import java.util.UUID;

public record ProviderCallback(
        UUID id,
        UUID workspaceId,
        UUID providerConfigId,
        String providerKey,
        String providerEventId,
        Long providerSequence,
        String callbackType,
        String businessReferenceType,
        UUID businessReferenceId,
        String payloadJson,
        String signatureHeader,
        boolean signatureVerified,
        String dedupeKey,
        String processingStatus,
        String processingError,
        Instant appliedAt,
        Instant receivedAt,
        Instant createdAt,
        Instant updatedAt) {
}
