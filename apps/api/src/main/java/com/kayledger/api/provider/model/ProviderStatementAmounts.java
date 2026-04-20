package com.kayledger.api.provider.model;

public record ProviderStatementAmounts(
        long settledGrossAmountMinor,
        long feeAmountMinor,
        long netEarningsAmountMinor,
        long payoutSucceededAmountMinor,
        long refundAmountMinor,
        long activeDisputeExposureAmountMinor,
        long settledSubscriptionNetRevenueAmountMinor) {
}
