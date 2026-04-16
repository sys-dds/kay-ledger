package com.kayledger.api.finance.model;

import java.util.UUID;

public record AccountBalance(
        UUID accountId,
        String currencyCode,
        long debitAmountMinor,
        long creditAmountMinor,
        long signedBalanceMinor) {
}
