package com.kayledger.api.reconciliation.application;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.provider.model.ProviderStatementAmounts;
import com.kayledger.api.provider.model.ProviderStatementTruth;
import com.kayledger.api.reconciliation.model.ProviderTruthImport;
import com.kayledger.api.reconciliation.model.ProviderTruthSnapshot;
import com.kayledger.api.reconciliation.model.ReconciliationItemAuditEvent;
import com.kayledger.api.reconciliation.model.ReconciliationItem;
import com.kayledger.api.reconciliation.model.ReconciliationMismatchType;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.reconciliation.store.ReconciliationStore;
import com.kayledger.api.reconciliation.store.ReconciliationStore.ProviderTruthSnapshotDraft;
import com.kayledger.api.reconciliation.store.ReconciliationStore.ReconciliationItemDraft;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.reporting.store.ReportingStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class ReconciliationService {

    private final ReconciliationStore reconciliationStore;
    private final ReportingStore reportingStore;
    private final AccessPolicy accessPolicy;
    private final RegionService regionService;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;

    public ReconciliationService(
            ReconciliationStore reconciliationStore,
            ReportingStore reportingStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            ObjectMapper objectMapper,
            InvestigationIndexingService investigationIndexingService) {
        this.reconciliationStore = reconciliationStore;
        this.reportingStore = reportingStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
    }

    @Transactional
    public ProviderTruthImport recordProviderTruth(AccessContext context, ProviderStatementTruth truth) {
        requireWrite(context, "provider truth import");
        if (truth == null) {
            throw new BadRequestException("provider truth is required.");
        }
        String currencyCode = requireCurrency(truth.currencyCode());
        String sourceReference = requireText(truth.sourceReference(), "sourceReference");
        if (truth.providerProfileId() == null || truth.statementPeriodStart() == null || truth.statementPeriodEnd() == null) {
            throw new BadRequestException("providerProfileId and statement period are required.");
        }
        ProviderTruthImport truthImport = reconciliationStore.createProviderTruthImport(
                context.workspaceId(),
                truth.providerProfileId(),
                currencyCode,
                truth.statementPeriodStart(),
                truth.statementPeriodEnd(),
                sourceReference,
                context.actorId());
        reconciliationStore.supersedeOlderProviderTruthImports(truthImport, context.actorId());
        reconciliationStore.recordProviderTruthImportEvent(context.workspaceId(), truthImport.id(), "RECORDED", context.actorId(), "Provider truth import recorded.");
        if (truth.amounts() != null) {
            ProviderStatementAmounts amounts = truth.amounts();
            reconciliationStore.createProviderTruthSnapshot(truthImport, new ProviderTruthSnapshotDraft(
                    amounts.settledGrossAmountMinor(),
                    amounts.feeAmountMinor(),
                    amounts.netEarningsAmountMinor(),
                    amounts.payoutSucceededAmountMinor(),
                    amounts.refundAmountMinor(),
                    amounts.activeDisputeExposureAmountMinor(),
                    amounts.settledSubscriptionNetRevenueAmountMinor(),
                    json(truth.providerPayload() == null ? Map.of() : truth.providerPayload())));
        }
        return truthImport;
    }

    @Transactional
    public ReconciliationRun createRunFromProviderTruth(AccessContext context, UUID truthImportId) {
        requireWrite(context, "provider reconciliation run");
        ProviderTruthImport truthImport = reconciliationStore.findProviderTruthImport(context.workspaceId(), truthImportId)
                .orElseThrow(() -> new NotFoundException("Provider truth import was not found."));
        if ("SUPERSEDED".equals(truthImport.status())) {
            throw new BadRequestException("Superseded provider truth imports cannot be reconciled.");
        }
        ReconciliationRun run = reconciliationStore.createRun(truthImport, context.actorId());
        Optional<ProviderTruthSnapshot> providerTruth = reconciliationStore.findProviderTruthSnapshot(context.workspaceId(), truthImport.id());
        Optional<ProviderFinancialSummary> internalSummary = reportingStore.providerSummaryForPeriod(
                context.workspaceId(),
                truthImport.providerProfileId(),
                truthImport.currencyCode(),
                truthImport.statementPeriodStart(),
                truthImport.statementPeriodEnd());

        int itemCount = compare(run, internalSummary.orElse(null), providerTruth.orElse(null));
        ReconciliationRun completed = reconciliationStore.completeRun(context.workspaceId(), run.id(), itemCount);
        reconciliationStore.markImportStatus(context.workspaceId(), truthImport.id(), itemCount == 0 ? "MATCHED" : "MISMATCHED", context.actorId());
        indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_RUN", completed.id());
        for (ReconciliationItem item : reconciliationStore.listItems(context.workspaceId(), completed.id(), false)) {
            indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_ITEM", item.id());
        }
        return completed;
    }

    public List<ReconciliationRun> listRuns(AccessContext context) {
        requireRead(context);
        return reconciliationStore.listRuns(context.workspaceId());
    }

    public ReconciliationRun runDetails(AccessContext context, UUID runId) {
        requireRead(context);
        return reconciliationStore.findRun(context.workspaceId(), runId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation run was not found."));
    }

    public ReconciliationRun executeRunForWorkflow(UUID workspaceId, UUID runId) {
        return reconciliationStore.findRun(workspaceId, runId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation run was not found."));
    }

    public ReconciliationRun startRun(AccessContext context, RunReconciliationCommand command) {
        if (command == null || command.truthImportId() == null) {
            throw new BadRequestException("truthImportId is required.");
        }
        return createRunFromProviderTruth(context, command.truthImportId());
    }

    public List<ReconciliationItem> listItems(AccessContext context, UUID runId, boolean unresolvedOnly) {
        requireRead(context);
        runDetails(context, runId);
        return reconciliationStore.listItems(context.workspaceId(), runId, unresolvedOnly);
    }

    public List<ReconciliationItemAuditEvent> listItemEvents(AccessContext context, UUID itemId) {
        requireRead(context);
        ReconciliationItem item = reconciliationStore.findItem(context.workspaceId(), itemId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation item was not found."));
        return reconciliationStore.listItemEvents(context.workspaceId(), item.id());
    }

    @Transactional
    public ReconciliationItem resolveItem(AccessContext context, UUID itemId, String resolutionOutcome, String resolutionNote) {
        requireWrite(context, "provider reconciliation resolution");
        requireText(resolutionNote, "resolutionNote");
        String outcome = resolutionOutcome == null || resolutionOutcome.isBlank() ? "OPERATOR_ACCEPTED" : resolutionOutcome.trim();
        ReconciliationItem existing = reconciliationStore.findItem(context.workspaceId(), itemId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation item was not found."));
        if (!"OPEN".equals(existing.status())) {
            throw new BadRequestException("Provider reconciliation item is not open.");
        }
        ReconciliationItem resolved = reconciliationStore.resolveItem(context.workspaceId(), itemId, context.actorId(), outcome, resolutionNote.trim());
        reconciliationStore.refreshRunCounts(context.workspaceId(), resolved.reconciliationRunId());
        refreshImportStatusAfterItemChange(context.workspaceId(), resolved.reconciliationRunId(), context.actorId());
        indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_ITEM", resolved.id());
        indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_RUN", resolved.reconciliationRunId());
        return resolved;
    }

    @Transactional
    public ReconciliationItem reopenItem(AccessContext context, UUID itemId, String reason) {
        requireWrite(context, "provider reconciliation reopen");
        requireText(reason, "reason");
        ReconciliationItem existing = reconciliationStore.findItem(context.workspaceId(), itemId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation item was not found."));
        if (!"RESOLVED".equals(existing.status())) {
            throw new BadRequestException("Provider reconciliation item is not resolved.");
        }
        ReconciliationItem reopened = reconciliationStore.reopenItem(context.workspaceId(), itemId, context.actorId(), reason.trim());
        reconciliationStore.refreshRunCounts(context.workspaceId(), reopened.reconciliationRunId());
        refreshImportStatusAfterItemChange(context.workspaceId(), reopened.reconciliationRunId(), context.actorId());
        indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_ITEM", reopened.id());
        indexReference(context.workspaceId(), "PROVIDER_RECONCILIATION_RUN", reopened.reconciliationRunId());
        return reopened;
    }

    private int compare(ReconciliationRun run, ProviderFinancialSummary internalSummary, ProviderTruthSnapshot providerTruth) {
        if (internalSummary == null && providerTruth == null) {
            return 0;
        }
        if (internalSummary == null) {
            createItem(run, ReconciliationMismatchType.MISSING_INTERNAL_SUMMARY, null, providerTruth, null, null);
            return 1;
        }
        if (providerTruth == null) {
            createItem(run, ReconciliationMismatchType.MISSING_PROVIDER_TRUTH, internalSummary, null, null, null);
            return 1;
        }

        int itemCount = 0;
        itemCount += compareAmount(run, ReconciliationMismatchType.SETTLED_GROSS_MISMATCH, internalSummary, providerTruth,
                "settledGrossAmountMinor", internalSummary.settledGrossAmountMinor(), providerTruth.settledGrossAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.FEE_MISMATCH, internalSummary, providerTruth,
                "feeAmountMinor", internalSummary.feeAmountMinor(), providerTruth.feeAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.NET_EARNINGS_MISMATCH, internalSummary, providerTruth,
                "netEarningsAmountMinor", internalSummary.netEarningsAmountMinor(), providerTruth.netEarningsAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.PAYOUT_MISMATCH, internalSummary, providerTruth,
                "payoutSucceededAmountMinor", internalSummary.payoutSucceededAmountMinor(), providerTruth.payoutSucceededAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.REFUND_MISMATCH, internalSummary, providerTruth,
                "refundAmountMinor", internalSummary.refundAmountMinor(), providerTruth.refundAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.DISPUTE_EXPOSURE_MISMATCH, internalSummary, providerTruth,
                "activeDisputeExposureAmountMinor", internalSummary.activeDisputeExposureAmountMinor(), providerTruth.activeDisputeExposureAmountMinor());
        itemCount += compareAmount(run, ReconciliationMismatchType.SUBSCRIPTION_REVENUE_MISMATCH, internalSummary, providerTruth,
                "settledSubscriptionNetRevenueAmountMinor", internalSummary.settledSubscriptionNetRevenueAmountMinor(), providerTruth.settledSubscriptionNetRevenueAmountMinor());
        return itemCount;
    }

    private void refreshImportStatusAfterItemChange(UUID workspaceId, UUID runId, UUID actorId) {
        ReconciliationRun run = reconciliationStore.findRun(workspaceId, runId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation run was not found."));
        if (run.unresolvedItemCount() == 0 && run.resolvedItemCount() > 0) {
            reconciliationStore.markImportStatus(workspaceId, run.truthImportId(), "RESOLVED", actorId);
        } else if (run.unresolvedItemCount() > 0) {
            reconciliationStore.markImportStatus(workspaceId, run.truthImportId(), "MISMATCHED", actorId);
        }
    }

    private int compareAmount(ReconciliationRun run, ReconciliationMismatchType mismatchType, ProviderFinancialSummary internalSummary, ProviderTruthSnapshot providerTruth, String fieldName, long internalValue, long providerValue) {
        if (internalValue == providerValue) {
            return 0;
        }
        createItem(run, mismatchType, internalSummary, providerTruth, fieldName, new AmountPair(internalValue, providerValue));
        return 1;
    }

    private void createItem(ReconciliationRun run, ReconciliationMismatchType mismatchType, ProviderFinancialSummary internalSummary, ProviderTruthSnapshot providerTruth, String fieldName, AmountPair amountPair) {
        reconciliationStore.createItem(run, new ReconciliationItemDraft(
                mismatchType.name(),
                internalSummary == null ? null : internalSummary.settledGrossAmountMinor(),
                providerTruth == null ? null : providerTruth.settledGrossAmountMinor(),
                internalSummary == null ? null : internalSummary.feeAmountMinor(),
                providerTruth == null ? null : providerTruth.feeAmountMinor(),
                internalSummary == null ? null : internalSummary.netEarningsAmountMinor(),
                providerTruth == null ? null : providerTruth.netEarningsAmountMinor(),
                internalSummary == null ? null : internalSummary.payoutSucceededAmountMinor(),
                providerTruth == null ? null : providerTruth.payoutSucceededAmountMinor(),
                internalSummary == null ? null : internalSummary.refundAmountMinor(),
                providerTruth == null ? null : providerTruth.refundAmountMinor(),
                internalSummary == null ? null : internalSummary.activeDisputeExposureAmountMinor(),
                providerTruth == null ? null : providerTruth.activeDisputeExposureAmountMinor(),
                internalSummary == null ? null : internalSummary.settledSubscriptionNetRevenueAmountMinor(),
                providerTruth == null ? null : providerTruth.settledSubscriptionNetRevenueAmountMinor(),
                detailJson(mismatchType, fieldName, amountPair)));
    }

    private String detailJson(ReconciliationMismatchType mismatchType, String fieldName, AmountPair amountPair) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("mismatchType", mismatchType.name());
        if (fieldName != null) {
            detail.put("field", fieldName);
            detail.put("internalValue", amountPair.internalValue());
            detail.put("providerValue", amountPair.providerValue());
        }
        return json(detail);
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireWrite(AccessContext context, String operation) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
        regionService.requireOwnedForWrite(context, operation);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new BadRequestException("provider reconciliation details could not be serialized.");
        }
    }

    private void indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        try {
            investigationIndexingService.indexReference(workspaceId, referenceType, referenceId);
        } catch (RuntimeException ignored) {
            // Reconciliation truth is durable in PostgreSQL and can be replayed by investigation reindex.
        }
    }

    private static String requireCurrency(String currencyCode) {
        if (currencyCode == null || !currencyCode.trim().matches("^[A-Za-z]{3}$")) {
            throw new BadRequestException("currencyCode must be a three-letter currency code.");
        }
        return currencyCode.trim().toUpperCase(Locale.ROOT);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(fieldName + " is required.");
        }
        return value.trim();
    }

    private record AmountPair(long internalValue, long providerValue) {
    }

    public record RunReconciliationCommand(String scope, UUID truthImportId) {
        public RunReconciliationCommand(UUID truthImportId) {
            this(null, truthImportId);
        }

        public RunReconciliationCommand(String scope) {
            this(scope, null);
        }
    }
}
