package com.kayledger.api.merchantevents.model;

import java.time.Instant;
import java.util.UUID;

public record MerchantFinanceEvent(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        UUID accountingPeriodId,
        String eventType,
        String sourceReferenceType,
        UUID sourceReferenceId,
        String payloadJson,
        String eventKey,
        Instant occurredAt,
        Instant createdAt) {
}
