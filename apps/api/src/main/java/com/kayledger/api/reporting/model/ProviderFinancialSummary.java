package com.kayledger.api.reporting.model;

import java.time.Instant;
import java.util.UUID;

public record ProviderFinancialSummary(
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        long settledGrossAmountMinor,
        long feeAmountMinor,
        long netEarningsAmountMinor,
        long currentPayoutRequestedAmountMinor,
        long payoutSucceededAmountMinor,
        long refundAmountMinor,
        long activeDisputeExposureAmountMinor,
        long settledSubscriptionNetRevenueAmountMinor,
        Instant refreshedAt) {
}
