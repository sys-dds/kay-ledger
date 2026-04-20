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
        String mismatchType,
        String currencyCode,
        Long amountMinor,
        String periodStart,
        String periodEnd,
        Instant occurredAt,
        Map<String, Object> data) {

    public InvestigationDocument(
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
        this(documentId, workspaceId, documentType, referenceType, referenceId, providerProfileId, paymentIntentId, refundId, payoutRequestId,
                disputeId, subscriptionId, providerEventId, externalReference, businessReferenceType, businessReferenceId, status,
                null, currencyCode, amountMinor, null, null, occurredAt, data);
    }
}
