package com.kayledger.api.finance.model;

import com.kayledger.api.shared.api.BadRequestException;

public record Money(String currencyCode, long amountMinor) {

    public Money {
        currencyCode = requireCurrency(currencyCode);
        if (amountMinor < 0) {
            throw new BadRequestException("amountMinor must be zero or greater.");
        }
    }

    public Money plus(Money other) {
        requireSameCurrency(other);
        return new Money(currencyCode, amountMinor + other.amountMinor);
    }

    public Money minus(Money other) {
        requireSameCurrency(other);
        if (amountMinor < other.amountMinor) {
            throw new BadRequestException("Money subtraction cannot go below zero.");
        }
        return new Money(currencyCode, amountMinor - other.amountMinor);
    }

    public static String requireCurrency(String currencyCode) {
        if (currencyCode == null || !currencyCode.matches("[A-Z]{3}")) {
            throw new BadRequestException("currencyCode must be a three-letter uppercase code.");
        }
        return currencyCode;
    }

    private void requireSameCurrency(Money other) {
        if (!currencyCode.equals(other.currencyCode())) {
            throw new BadRequestException("Money currencies must match.");
        }
    }
}
