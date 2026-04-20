package com.kayledger.api.reconciliation.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record ProviderTruthImport(
        UUID id,
        UUID workspaceId,
        UUID providerProfileId,
        String currencyCode,
        LocalDate statementPeriodStart,
        LocalDate statementPeriodEnd,
        String sourceReference,
        String sourceType,
        int importVersion,
        String status,
        UUID supersededByImportId,
        Instant supersededAt,
        UUID importedByActorId,
        Instant importedAt,
        Instant createdAt,
        Instant updatedAt) {
}
