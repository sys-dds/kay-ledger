package com.kayledger.api.reconciliation.api;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import com.kayledger.api.provider.model.ProviderStatementAmounts;
import com.kayledger.api.provider.model.ProviderStatementTruth;

public final class ReconciliationRequests {

    private ReconciliationRequests() {
    }

    public record ProviderTruthImportRequest(
            UUID providerProfileId,
            String currencyCode,
            LocalDate statementPeriodStart,
            LocalDate statementPeriodEnd,
            String sourceReference,
            ProviderStatementAmounts amounts,
            Map<String, Object> providerPayload) {

        public ProviderStatementTruth toTruth() {
            return new ProviderStatementTruth(
                    providerProfileId,
                    currencyCode,
                    statementPeriodStart,
                    statementPeriodEnd,
                    sourceReference,
                    amounts,
                    providerPayload);
        }
    }

    public record CreateReconciliationRunRequest(UUID truthImportId) {
    }

    public record ResolveReconciliationItemRequest(String resolutionOutcome, String resolutionNote) {
    }

    public record ReopenReconciliationItemRequest(String reason) {
    }
}
