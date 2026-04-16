package com.kayledger.api.finance.model;

public record FeeBreakdown(Money grossAmount, Money feeAmount, Money netAmount) {
}
