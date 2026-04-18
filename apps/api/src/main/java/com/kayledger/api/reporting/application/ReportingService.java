package com.kayledger.api.reporting.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.ObjectProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.reporting.model.ExportArtifact;
import com.kayledger.api.reporting.model.ExportJob;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.reporting.store.ReportingStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.InternalFailureException;
import com.kayledger.api.temporal.application.OperatorWorkflowRecord;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.application.OperatorWorkflowStarter;
import com.kayledger.api.temporal.workflow.ExportOperatorWorkflow;
import com.kayledger.api.temporal.workflow.OperatorWorkflowInput;

import io.temporal.client.WorkflowClient;

@Service
public class ReportingService {

    private static final String PROVIDER_STATEMENT = "PROVIDER_STATEMENT";
    private static final String CSV = "text/csv";

    private final ReportingStore reportingStore;
    private final ObjectStorageService objectStorageService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;
    private final ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter;
    private final OperatorWorkflowService operatorWorkflowService;

    public ReportingService(
            ReportingStore reportingStore,
            ObjectStorageService objectStorageService,
            AccessPolicy accessPolicy,
            ObjectMapper objectMapper,
            InvestigationIndexingService investigationIndexingService,
            ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter,
            OperatorWorkflowService operatorWorkflowService) {
        this.reportingStore = reportingStore;
        this.objectStorageService = objectStorageService;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
        this.operatorWorkflowStarter = operatorWorkflowStarter;
        this.operatorWorkflowService = operatorWorkflowService;
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
    public ExportJob startExport(AccessContext context, ExportRequestCommand command) {
        requireWrite(context);
        OperatorWorkflowStarter workflowStarter = operatorWorkflowStarter.getIfAvailable();
        if (workflowStarter == null) {
            throw new InternalFailureException("Temporal export orchestration is not available.", new IllegalStateException("Temporal workflow starter bean is missing."));
        }
        String exportType = command == null || command.exportType() == null ? PROVIDER_STATEMENT : command.exportType();
        if (!PROVIDER_STATEMENT.equals(exportType)) {
            throw new BadRequestException("Only provider statement export is supported in this slice.");
        }
        try {
            String parametersJson = objectMapper.writeValueAsString(command == null ? new ExportRequestCommand(PROVIDER_STATEMENT) : command);
            ExportJob job = reportingStore.createAsyncExportJob(context.workspaceId(), exportType, context.actorId(), parametersJson);
            OperatorWorkflowRecord workflow = operatorWorkflowService.createRequested(
                    context.workspaceId(),
                    OperatorWorkflowService.EXPORT,
                    OperatorWorkflowService.EXPORT_JOB,
                    job.id(),
                    OperatorWorkflowService.API,
                    context.actorId(),
                    1,
                    "Export requested.");
            String runId = workflowStarter.start(workflow.workflowId(), (client, options) -> {
                ExportOperatorWorkflow exportWorkflow = client.newWorkflowStub(ExportOperatorWorkflow.class, options);
                return WorkflowClient.start(exportWorkflow::run, new OperatorWorkflowInput(context.workspaceId(), job.id(), workflow.workflowId()));
            });
            operatorWorkflowService.attachRun(context.workspaceId(), workflow.workflowId(), runId);
            ExportJob tracked = reportingStore.attachWorkflow(context.workspaceId(), job.id(), workflow.workflowId(), runId);
            indexReference(context.workspaceId(), "EXPORT_JOB", tracked.id());
            return tracked;
        } catch (BadRequestException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new InternalFailureException("Export orchestration could not be started.", exception);
        }
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ExportJob generateSynchronousExport(AccessContext context, ExportRequestCommand command) {
        requireWrite(context);
        String exportType = command == null || command.exportType() == null ? PROVIDER_STATEMENT : command.exportType();
        if (!PROVIDER_STATEMENT.equals(exportType)) {
            throw new BadRequestException("Only provider statement export is supported in this slice.");
        }
        ExportJob job = null;
        try {
            String parametersJson = objectMapper.writeValueAsString(command == null ? new ExportRequestCommand(PROVIDER_STATEMENT) : command);
            job = reportingStore.createExportJob(context.workspaceId(), exportType, context.actorId(), parametersJson);
            return generateExportJob(context.workspaceId(), job.id());
        } catch (Exception exception) {
            if (job != null) {
                reportingStore.markFailed(context.workspaceId(), job.id(), exception.getMessage());
                indexReference(context.workspaceId(), "EXPORT_JOB", job.id());
            }
            throw new InternalFailureException("Export generation or artifact storage failed; retry is required.", exception);
        }
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ExportJob generateExportForWorkflow(UUID workspaceId, UUID jobId) {
        return generateExportJob(workspaceId, jobId);
    }

    private ExportJob generateExportJob(UUID workspaceId, UUID jobId) {
        ExportJob job = reportingStore.findJob(workspaceId, jobId);
        try {
            reportingStore.markRunning(workspaceId, job.id());
            reportingStore.refreshProviderSummaries(workspaceId);
            List<ProviderFinancialSummary> summaries = reportingStore.listProviderSummaries(workspaceId);
            byte[] content = providerStatementCsv(summaries).getBytes(StandardCharsets.UTF_8);
            String storageKey = "exports/" + workspaceId + "/" + job.id() + "/provider-statement.csv";
            objectStorageService.put(storageKey, CSV, content);
            ExportArtifact artifact = reportingStore.createArtifact(workspaceId, job.id(), storageKey, CSV, content.length, summaries.size(), sha256(content));
            ExportJob succeeded = reportingStore.markSucceeded(workspaceId, job.id(), summaries.size(), storageKey, CSV);
            indexReference(workspaceId, "EXPORT_JOB", succeeded.id());
            indexReference(workspaceId, "EXPORT_ARTIFACT", artifact.id());
            return succeeded;
        } catch (Exception exception) {
            reportingStore.markFailed(workspaceId, job.id(), exception.getMessage());
            indexReference(workspaceId, "EXPORT_JOB", job.id());
            throw new InternalFailureException("Export generation or artifact storage failed; retry is required.", exception);
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
        builder.append("provider_profile_id,currency_code,settled_gross,fees,net_earnings,current_payout_requested,payout_succeeded,settled_refunds,active_dispute_exposure,settled_subscription_net_revenue,refreshed_at\n");
        for (ProviderFinancialSummary summary : summaries) {
            builder.append(summary.providerProfileId()).append(',')
                    .append(summary.currencyCode()).append(',')
                    .append(summary.settledGrossAmountMinor()).append(',')
                    .append(summary.feeAmountMinor()).append(',')
                    .append(summary.netEarningsAmountMinor()).append(',')
                    .append(summary.currentPayoutRequestedAmountMinor()).append(',')
                    .append(summary.payoutSucceededAmountMinor()).append(',')
                    .append(summary.refundAmountMinor()).append(',')
                    .append(summary.activeDisputeExposureAmountMinor()).append(',')
                    .append(summary.settledSubscriptionNetRevenueAmountMinor()).append(',')
                    .append(summary.refreshedAt() == null ? Instant.EPOCH : summary.refreshedAt())
                    .append('\n');
        }
        return builder.toString();
    }

    private void indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        try {
            investigationIndexingService.indexReference(workspaceId, referenceType, referenceId);
        } catch (RuntimeException ignored) {
            // Export metadata is durable in PostgreSQL; operator search can be repaired by reindex.
        }
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
