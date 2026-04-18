package com.kayledger.api.reporting.api;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
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

@RestController
@RequestMapping("/api/reporting")
public class ReportingController {

    private final AccessContextResolver accessContextResolver;
    private final ReportingService reportingService;
    private final IdempotencyService idempotencyService;

    public ReportingController(AccessContextResolver accessContextResolver, ReportingService reportingService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.reportingService = reportingService;
        this.idempotencyService = idempotencyService;
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
    ResponseEntity<Object> generateSynchronousExport(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) ExportRequestCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(idempotencyKey, "WORKSPACE", context.workspaceId(), context.actorId(), "POST /api/reporting/exports", IdempotencyService.fingerprint(workspaceSlug, actorKey, request), () -> reportingService.generateSynchronousExport(context, request));
    }

    @GetMapping("/exports/jobs")
    List<ExportJob> jobs(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reportingService.listJobs(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/exports/artifacts")
    List<ExportArtifact> artifacts(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reportingService.listArtifacts(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }
}
