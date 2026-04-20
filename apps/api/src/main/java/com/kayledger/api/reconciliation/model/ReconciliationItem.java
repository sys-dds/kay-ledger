package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.util.UUID;

public record ReconciliationItem(
        UUID id,
        UUID workspaceId,
        UUID reconciliationRunId,
        UUID truthImportId,
        UUID providerProfileId,
        String currencyCode,
        String sourceReference,
        String mismatchType,
        Long internalSettledGrossAmountMinor,
        Long providerSettledGrossAmountMinor,
        Long internalFeeAmountMinor,
        Long providerFeeAmountMinor,
        Long internalNetEarningsAmountMinor,
        Long providerNetEarningsAmountMinor,
        Long internalPayoutSucceededAmountMinor,
        Long providerPayoutSucceededAmountMinor,
        Long internalRefundAmountMinor,
        Long providerRefundAmountMinor,
        Long internalActiveDisputeExposureAmountMinor,
        Long providerActiveDisputeExposureAmountMinor,
        Long internalSettledSubscriptionNetRevenueAmountMinor,
        Long providerSettledSubscriptionNetRevenueAmountMinor,
        String detailJson,
        String status,
        String resolutionOutcome,
        String resolutionNote,
        UUID resolvedByActorId,
        Instant resolvedAt,
        Instant createdAt,
        Instant updatedAt) {
}
