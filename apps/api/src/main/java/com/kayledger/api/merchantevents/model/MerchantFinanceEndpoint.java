package com.kayledger.api.merchantevents.model;

import java.time.Instant;
import java.util.UUID;

public record MerchantFinanceEndpoint(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String endpointUrl,
        String signingSecretRef,
        String status,
        String[] eventTypes,
        UUID createdByActorId,
        Instant createdAt,
        Instant updatedAt) {
}
