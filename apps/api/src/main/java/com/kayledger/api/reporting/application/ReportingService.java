package com.kayledger.api.reporting.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.reporting.model.ExportArtifact;
import com.kayledger.api.reporting.model.ExportJob;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.reporting.store.ReportingStore;
import com.kayledger.api.shared.api.BadRequestException;

@Service
public class ReportingService {

    private static final String PROVIDER_STATEMENT = "PROVIDER_STATEMENT";
    private static final String CSV = "text/csv";

    private final ReportingStore reportingStore;
    private final ObjectStorageService objectStorageService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public ReportingService(ReportingStore reportingStore, ObjectStorageService objectStorageService, AccessPolicy accessPolicy, ObjectMapper objectMapper) {
        this.reportingStore = reportingStore;
        this.objectStorageService = objectStorageService;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public List<ProviderFinancialSummary> refreshAndListSummaries(AccessContext context) {
        requireRead(context);
        reportingStore.refreshProviderSummaries(context.workspaceId());
        return reportingStore.listProviderSummaries(context.workspaceId());
    }

    public List<ProviderFinancialSummary> listSummaries(AccessContext context) {
        requireRead(context);
        return reportingStore.listProviderSummaries(context.workspaceId());
    }

    @Transactional
    public ExportJob requestExport(AccessContext context, ExportRequestCommand command) {
        requireWrite(context);
        String exportType = command == null || command.exportType() == null ? PROVIDER_STATEMENT : command.exportType();
        if (!PROVIDER_STATEMENT.equals(exportType)) {
            throw new BadRequestException("Only provider statement export is supported in this slice.");
        }
        ExportJob job = null;
        try {
            String parametersJson = objectMapper.writeValueAsString(command == null ? new ExportRequestCommand(PROVIDER_STATEMENT) : command);
            job = reportingStore.createExportJob(context.workspaceId(), exportType, context.actorId(), parametersJson);
            reportingStore.markRunning(context.workspaceId(), job.id());
            reportingStore.refreshProviderSummaries(context.workspaceId());
            List<ProviderFinancialSummary> summaries = reportingStore.listProviderSummaries(context.workspaceId());
            byte[] content = providerStatementCsv(summaries).getBytes(StandardCharsets.UTF_8);
            String storageKey = "exports/" + context.workspaceId() + "/" + job.id() + "/provider-statement.csv";
            objectStorageService.put(storageKey, CSV, content);
            reportingStore.createArtifact(context.workspaceId(), job.id(), storageKey, CSV, content.length, summaries.size(), sha256(content));
            return reportingStore.markSucceeded(context.workspaceId(), job.id(), summaries.size(), storageKey, CSV);
        } catch (Exception exception) {
            if (job != null) {
                reportingStore.markFailed(context.workspaceId(), job.id(), exception.getMessage());
            }
            throw new BadRequestException("Export could not be generated: " + exception.getMessage());
        }
    }

    public List<ExportJob> listJobs(AccessContext context) {
        requireRead(context);
        return reportingStore.listJobs(context.workspaceId());
    }

    public List<ExportArtifact> listArtifacts(AccessContext context) {
        requireRead(context);
        return reportingStore.listArtifacts(context.workspaceId());
    }

    private static String providerStatementCsv(List<ProviderFinancialSummary> summaries) {
        StringBuilder builder = new StringBuilder();
        builder.append("provider_profile_id,currency_code,settled_gross,fees,net_earnings,payout_requested,payout_succeeded,refunds,disputes,subscription_revenue,refreshed_at\n");
        for (ProviderFinancialSummary summary : summaries) {
            builder.append(summary.providerProfileId()).append(',')
                    .append(summary.currencyCode()).append(',')
                    .append(summary.settledGrossAmountMinor()).append(',')
                    .append(summary.feeAmountMinor()).append(',')
                    .append(summary.netEarningsAmountMinor()).append(',')
                    .append(summary.payoutRequestedAmountMinor()).append(',')
                    .append(summary.payoutSucceededAmountMinor()).append(',')
                    .append(summary.refundAmountMinor()).append(',')
                    .append(summary.disputeAmountMinor()).append(',')
                    .append(summary.subscriptionRevenueAmountMinor()).append(',')
                    .append(summary.refreshedAt() == null ? Instant.EPOCH : summary.refreshedAt())
                    .append('\n');
        }
        return builder.toString();
    }

    private static String sha256(byte[] content) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
    }

    public record ExportRequestCommand(String exportType) {
    }
}
