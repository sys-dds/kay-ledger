package com.kayledger.api.close.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record FinalizedProviderStatement(
        UUID id,
        UUID workspaceId,
        UUID accountingPeriodId,
        UUID providerProfileId,
        String currencyCode,
        LocalDate periodStart,
        LocalDate periodEnd,
        String status,
        long settledGrossAmountMinor,
        long feeAmountMinor,
        long netEarningsAmountMinor,
        long currentPayoutRequestedAmountMinor,
        long payoutSucceededAmountMinor,
        long refundAmountMinor,
        long activeDisputeExposureAmountMinor,
        long settledSubscriptionNetRevenueAmountMinor,
        String snapshotJson,
        UUID finalizedByActorId,
        Instant finalizedAt,
        Instant reopenedAt,
        Instant createdAt,
        Instant updatedAt) {
}
