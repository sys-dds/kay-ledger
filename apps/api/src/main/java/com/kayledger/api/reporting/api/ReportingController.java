package com.kayledger.api.reporting.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.reporting.application.ReportingService.ExportRequestCommand;
import com.kayledger.api.reporting.model.ExportArtifact;
import com.kayledger.api.reporting.model.ExportJob;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.temporal.application.OperatorWorkflowQueryService;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.model.OperatorWorkflowStatus;

@RestController
@RequestMapping("/api/reporting")
public class ReportingController {

    private final AccessContextResolver accessContextResolver;
    private final ReportingService reportingService;
    private final IdempotencyService idempotencyService;
    private final OperatorWorkflowQueryService operatorWorkflowQueryService;

    public ReportingController(AccessContextResolver accessContextResolver, ReportingService reportingService, IdempotencyService idempotencyService, OperatorWorkflowQueryService operatorWorkflowQueryService) {
        this.accessContextResolver = accessContextResolver;
        this.reportingService = reportingService;
        this.idempotencyService = idempotencyService;
        this.operatorWorkflowQueryService = operatorWorkflowQueryService;
    }

    @GetMapping("/summaries/providers")
    List<ProviderFinancialSummary> summaries(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reportingService.listSummaries(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/summaries/providers/refresh")
    ResponseEntity<Object> refreshSummaries(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(idempotencyKey, "WORKSPACE", context.workspaceId(), context.actorId(), "POST /api/reporting/summaries/providers/refresh", IdempotencyService.fingerprint(workspaceSlug, actorKey, "refresh-provider-summaries"), () -> reportingService.refreshAndListSummaries(context));
    }

    @PostMapping("/exports")
    ResponseEntity<Object> startExport(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) ExportRequestCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(idempotencyKey, "WORKSPACE", context.workspaceId(), context.actorId(), "POST /api/reporting/exports", IdempotencyService.fingerprint(workspaceSlug, actorKey, request), () -> reportingService.startExport(context, request));
    }

    @GetMapping("/exports/jobs")
    List<ExportJob> jobs(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reportingService.listJobs(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/exports/jobs/{jobId}/workflow")
    OperatorWorkflowStatus exportWorkflow(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID jobId) {
        return operatorWorkflowQueryService.findByReference(
                accessContextResolver.resolveWorkspace(workspaceSlug, actorKey),
                OperatorWorkflowService.EXPORT,
                OperatorWorkflowService.EXPORT_JOB,
                jobId);
    }

    @GetMapping("/exports/artifacts")
    List<ExportArtifact> artifacts(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reportingService.listArtifacts(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }
}
