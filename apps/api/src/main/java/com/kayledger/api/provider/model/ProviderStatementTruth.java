package com.kayledger.api.provider.model;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

public record ProviderStatementTruth(
        UUID providerProfileId,
        String currencyCode,
        LocalDate statementPeriodStart,
        LocalDate statementPeriodEnd,
        String sourceReference,
        ProviderStatementAmounts amounts,
        Map<String, Object> providerPayload) {
}
