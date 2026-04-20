package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record ProviderTruthSnapshot(
        UUID id,
        UUID workspaceId,
        UUID truthImportId,
        UUID providerProfileId,
        String currencyCode,
        LocalDate statementPeriodStart,
        LocalDate statementPeriodEnd,
        String sourceReference,
        long settledGrossAmountMinor,
        long feeAmountMinor,
        long netEarningsAmountMinor,
        long payoutSucceededAmountMinor,
        long refundAmountMinor,
        long activeDisputeExposureAmountMinor,
        long settledSubscriptionNetRevenueAmountMinor,
        String providerPayloadJson,
        Instant createdAt,
        Instant updatedAt) {
}
