package com.kayledger.api.investigation.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record InvestigationDocument(
        String documentId,
        UUID workspaceId,
        String documentType,
        String referenceType,
        UUID referenceId,
        UUID providerProfileId,
        UUID paymentIntentId,
        UUID refundId,
        UUID payoutRequestId,
        UUID disputeId,
        UUID subscriptionId,
        String providerEventId,
        String externalReference,
        String businessReferenceType,
        UUID businessReferenceId,
        String status,
        String currencyCode,
        Long amountMinor,
        Instant occurredAt,
        Map<String, Object> data) {
}
