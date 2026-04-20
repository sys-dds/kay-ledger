package com.kayledger.api.evidence.model;

import java.time.Instant;
import java.util.UUID;

public record FinanceEvidencePack(
        UUID id,
        UUID workspaceId,
        String evidencePackType,
        String status,
        UUID accountingPeriodId,
        UUID finalizedStatementId,
        UUID reconciliationRunId,
        UUID providerTruthImportId,
        UUID approvalRequestId,
        String sourceReference,
        UUID generatedByActorId,
        String snapshotJson,
        Instant createdAt,
        Instant updatedAt) {
}
