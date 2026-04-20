package com.kayledger.api.close.application;

import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.approval.application.FinancialApprovalService;
import com.kayledger.api.close.model.AccountingPeriod;
import com.kayledger.api.close.model.FinalizedProviderStatement;
import com.kayledger.api.close.model.FinancialCloseAuditEvent;
import com.kayledger.api.close.store.FinancialCloseStore;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.reporting.store.ReportingStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class FinancialCloseService {

    private final FinancialCloseStore financialCloseStore;
    private final ReportingStore reportingStore;
    private final AccessPolicy accessPolicy;
    private final RegionService regionService;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;
    private final FinancialApprovalService financialApprovalService;
    private final MerchantFinanceEventService merchantFinanceEventService;

    public FinancialCloseService(
            FinancialCloseStore financialCloseStore,
            ReportingStore reportingStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            ObjectMapper objectMapper,
            InvestigationIndexingService investigationIndexingService,
            FinancialApprovalService financialApprovalService,
            MerchantFinanceEventService merchantFinanceEventService) {
        this.financialCloseStore = financialCloseStore;
        this.reportingStore = reportingStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
        this.financialApprovalService = financialApprovalService;
        this.merchantFinanceEventService = merchantFinanceEventService;
    }

    @Transactional
    public AccountingPeriod openPeriod(AccessContext context, LocalDate periodStart, LocalDate periodEnd) {
        requireWrite(context, "financial period open");
        validatePeriod(periodStart, periodEnd);
        if (financialCloseStore.countOverlappingPeriods(context.workspaceId(), periodStart, periodEnd) > 0) {
            throw new BadRequestException("Accounting period overlaps an existing period.");
        }
        AccountingPeriod period = financialCloseStore.createPeriod(context.workspaceId(), periodStart, periodEnd, context.actorId());
        indexReference(context.workspaceId(), "ACCOUNTING_PERIOD", period.id());
        return period;
    }

    public List<AccountingPeriod> listPeriods(AccessContext context) {
        requireRead(context);
        return financialCloseStore.listPeriods(context.workspaceId());
    }

    public AccountingPeriod periodDetails(AccessContext context, UUID periodId) {
        requireRead(context);
        return period(context.workspaceId(), periodId);
    }

    @Transactional
    public CloseResult closePeriod(AccessContext context, UUID periodId, String reason) {
        return closePeriod(context, periodId, reason, null);
    }

    @Transactional
    public CloseResult closePeriod(AccessContext context, UUID periodId, String reason, UUID approvalRequestId) {
        requireWrite(context, "financial period close");
        String closeReason = requireText(reason, "closeReason");
        AccountingPeriod existing = period(context.workspaceId(), periodId);
        if (!"OPEN".equals(existing.status())) {
            throw new BadRequestException("Only open accounting periods can be closed.");
        }
        if (financialApprovalService.closeRequiresApproval()) {
            financialApprovalService.requireApprovedForExecution(
                    context,
                    approvalRequestId,
                    FinancialApprovalService.ACTION_FINANCIAL_PERIOD_CLOSE,
                    "ACCOUNTING_PERIOD",
                    existing.id());
        }
        AccountingPeriod closed = financialCloseStore.closePeriod(context.workspaceId(), existing.id(), context.actorId(), closeReason);
        financialCloseStore.createCloseRecord(context.workspaceId(), closed.id(), context.actorId(), closeReason);
        List<FinalizedProviderStatement> statements = reportingStore.listProviderSummariesForPeriod(context.workspaceId(), closed.periodStart(), closed.periodEnd())
                .stream()
                .map(summary -> financialCloseStore.createFinalizedStatement(closed, summary, context.actorId(), snapshotJson(closed, summary)))
                .toList();
        indexReference(context.workspaceId(), "ACCOUNTING_PERIOD", closed.id());
        for (FinalizedProviderStatement statement : statements) {
            indexReference(context.workspaceId(), "FINALIZED_PROVIDER_STATEMENT", statement.id());
            merchantFinanceEventService.emit(
                    context.workspaceId(),
                    statement.providerProfileId(),
                    statement.currencyCode(),
                    statement.accountingPeriodId(),
                    MerchantFinanceEventService.EVENT_FINALIZED_STATEMENT_AVAILABLE,
                    "FINALIZED_PROVIDER_STATEMENT",
                    statement.id(),
                    statementPayload(statement));
        }
        return new CloseResult(closed, statements);
    }

    @Transactional
    public AccountingPeriod reopenPeriod(AccessContext context, UUID periodId, String reason) {
        return reopenPeriod(context, periodId, reason, null);
    }

    @Transactional
    public AccountingPeriod reopenPeriod(AccessContext context, UUID periodId, String reason, UUID approvalRequestId) {
        requireWrite(context, "financial period reopen");
        requireText(reason, "reopenReason");
        AccountingPeriod existing = period(context.workspaceId(), periodId);
        if (!"CLOSED".equals(existing.status())) {
            throw new BadRequestException("Only closed accounting periods can be reopened.");
        }
        financialApprovalService.requireApprovedForExecution(
                context,
                approvalRequestId,
                FinancialApprovalService.ACTION_FINANCIAL_PERIOD_REOPEN,
                "ACCOUNTING_PERIOD",
                existing.id());
        AccountingPeriod reopened = financialCloseStore.reopenPeriod(context.workspaceId(), existing.id(), context.actorId(), reason.trim());
        indexReference(context.workspaceId(), "ACCOUNTING_PERIOD", reopened.id());
        merchantFinanceEventService.emit(
                context.workspaceId(),
                null,
                null,
                reopened.id(),
                MerchantFinanceEventService.EVENT_ACCOUNTING_PERIOD_REOPENED,
                "ACCOUNTING_PERIOD",
                reopened.id(),
                periodPayload(reopened));
        return reopened;
    }

    public List<FinalizedProviderStatement> finalizedStatements(AccessContext context, UUID periodId) {
        requireRead(context);
        period(context.workspaceId(), periodId);
        return financialCloseStore.listFinalizedStatements(context.workspaceId(), periodId);
    }

    public FinalizedProviderStatement finalizedStatementDetails(AccessContext context, UUID statementId) {
        requireRead(context);
        return financialCloseStore.findFinalizedStatement(context.workspaceId(), statementId)
                .orElseThrow(() -> new NotFoundException("Finalized provider statement was not found."));
    }

    public List<FinancialCloseAuditEvent> auditEvents(AccessContext context, UUID periodId) {
        requireRead(context);
        period(context.workspaceId(), periodId);
        return financialCloseStore.listAuditEvents(context.workspaceId(), periodId);
    }

    public void requireOpenForPosting(UUID workspaceId, UUID providerProfileId, String currencyCode, Instant occurredAt, String operation) {
        financialCloseStore.closedPeriodForPosting(workspaceId, occurredAt)
                .ifPresent(period -> {
                    throw new BadRequestException(operation + " cannot post into closed accounting period " + period.periodStart() + " to " + period.periodEnd() + ".");
                });
    }

    private AccountingPeriod period(UUID workspaceId, UUID periodId) {
        if (periodId == null) {
            throw new BadRequestException("periodId is required.");
        }
        return financialCloseStore.findPeriod(workspaceId, periodId)
                .orElseThrow(() -> new NotFoundException("Accounting period was not found."));
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

    private String snapshotJson(AccountingPeriod period, ProviderFinancialSummary summary) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("accountingPeriodId", period.id());
        snapshot.put("periodStart", period.periodStart());
        snapshot.put("periodEnd", period.periodEnd());
        snapshot.put("providerProfileId", summary.providerProfileId());
        snapshot.put("currencyCode", summary.currencyCode());
        snapshot.put("settledGrossAmountMinor", summary.settledGrossAmountMinor());
        snapshot.put("feeAmountMinor", summary.feeAmountMinor());
        snapshot.put("netEarningsAmountMinor", summary.netEarningsAmountMinor());
        snapshot.put("currentPayoutRequestedAmountMinor", summary.currentPayoutRequestedAmountMinor());
        snapshot.put("payoutSucceededAmountMinor", summary.payoutSucceededAmountMinor());
        snapshot.put("refundAmountMinor", summary.refundAmountMinor());
        snapshot.put("activeDisputeExposureAmountMinor", summary.activeDisputeExposureAmountMinor());
        snapshot.put("settledSubscriptionNetRevenueAmountMinor", summary.settledSubscriptionNetRevenueAmountMinor());
        try {
            return objectMapper.writeValueAsString(snapshot);
        } catch (Exception exception) {
            throw new BadRequestException("Finalized provider statement snapshot could not be serialized.");
        }
    }

    private Map<String, Object> statementPayload(FinalizedProviderStatement statement) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("finalizedStatementId", statement.id());
        payload.put("accountingPeriodId", statement.accountingPeriodId());
        payload.put("providerProfileId", statement.providerProfileId());
        payload.put("currencyCode", statement.currencyCode());
        payload.put("periodStart", statement.periodStart());
        payload.put("periodEnd", statement.periodEnd());
        payload.put("status", statement.status());
        return payload;
    }

    private Map<String, Object> periodPayload(AccountingPeriod period) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accountingPeriodId", period.id());
        payload.put("periodStart", period.periodStart());
        payload.put("periodEnd", period.periodEnd());
        payload.put("status", period.status());
        return payload;
    }

    private static void validatePeriod(LocalDate periodStart, LocalDate periodEnd) {
        if (periodStart == null || periodEnd == null) {
            throw new BadRequestException("periodStart and periodEnd are required.");
        }
        if (periodEnd.isBefore(periodStart)) {
            throw new BadRequestException("periodEnd must be on or after periodStart.");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private void indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        try {
            investigationIndexingService.indexReference(workspaceId, referenceType, referenceId);
        } catch (RuntimeException ignored) {
            // PostgreSQL remains the close source of truth; investigation indexing can replay later.
        }
    }

    public record CloseResult(AccountingPeriod period, List<FinalizedProviderStatement> finalizedStatements) {
    }
}
