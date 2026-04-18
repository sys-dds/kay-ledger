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
        long payoutRequestedAmountMinor,
        long payoutSucceededAmountMinor,
        long refundAmountMinor,
        long disputeAmountMinor,
        long subscriptionRevenueAmountMinor,
        Instant refreshedAt) {
}
