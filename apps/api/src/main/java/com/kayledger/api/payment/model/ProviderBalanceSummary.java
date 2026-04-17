package com.kayledger.api.payment.model;

import java.util.UUID;

public record ProviderBalanceSummary(
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        long payableAmountMinor,
        long pendingPayoutAmountMinor,
        long paidOutAmountMinor,
        long refundedAmountMinor,
        long frozenAmountMinor,
        long availableAmountMinor) {
}
