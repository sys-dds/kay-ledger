package com.kayledger.api.investigation.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.investigation.application.InvestigationSearchService;
import com.kayledger.api.investigation.application.InvestigationSearchService.SearchCommand;
import com.kayledger.api.investigation.model.InvestigationSearchHit;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.model.RegionReadFreshness;
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.temporal.application.OperatorWorkflowQueryService;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.model.OperatorWorkflowStatus;

@RestController
@RequestMapping("/api/investigation")
public class InvestigationController {

    private final AccessContextResolver accessContextResolver;
    private final InvestigationSearchService investigationSearchService;
    private final InvestigationIndexingService investigationIndexingService;
    private final OperatorWorkflowQueryService operatorWorkflowQueryService;
    private final IdempotencyService idempotencyService;
    private final RegionReplicationService regionReplicationService;

    public InvestigationController(AccessContextResolver accessContextResolver, InvestigationSearchService investigationSearchService, InvestigationIndexingService investigationIndexingService, OperatorWorkflowQueryService operatorWorkflowQueryService, IdempotencyService idempotencyService, RegionReplicationService regionReplicationService) {
        this.accessContextResolver = accessContextResolver;
        this.investigationSearchService = investigationSearchService;
        this.investigationIndexingService = investigationIndexingService;
        this.operatorWorkflowQueryService = operatorWorkflowQueryService;
        this.idempotencyService = idempotencyService;
        this.regionReplicationService = regionReplicationService;
    }

    @GetMapping("/search")
    ResponseEntity<List<InvestigationSearchHit>> search(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestParam(required = false) String paymentId,
            @RequestParam(required = false) String refundId,
            @RequestParam(required = false) String payoutId,
            @RequestParam(required = false) String disputeId,
            @RequestParam(required = false) String providerEventId,
            @RequestParam(required = false) String externalReference,
            @RequestParam(required = false) String businessReferenceId,
            @RequestParam(required = false) String subscriptionId,
            @RequestParam(required = false) String providerProfileId,
            @RequestParam(required = false) String referenceId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return withFreshness(
                investigationSearchService.search(context, new SearchCommand(paymentId, refundId, payoutId, disputeId, providerEventId, externalReference, businessReferenceId, subscriptionId, providerProfileId, referenceId)),
                regionReplicationService.freshness(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, context.workspaceId()));
    }

    @GetMapping("/references/{referenceId}")
    ResponseEntity<List<InvestigationSearchHit>> byReference(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String referenceId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return withFreshness(
                investigationSearchService.byReference(context, referenceId),
                regionReplicationService.freshness(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, context.workspaceId()));
    }

    @GetMapping("/provider-events/{providerEventId}")
    ResponseEntity<List<InvestigationSearchHit>> byProviderEvent(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String providerEventId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return withFreshness(
                investigationSearchService.byProviderEvent(context, providerEventId),
                regionReplicationService.freshness(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, context.workspaceId()));
    }

    @GetMapping("/external-references/{externalReference}")
    ResponseEntity<List<InvestigationSearchHit>> byExternalReference(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String externalReference) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return withFreshness(
                investigationSearchService.byExternalReference(context, externalReference),
                regionReplicationService.freshness(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, context.workspaceId()));
    }

    @PostMapping("/reindex")
    ResponseEntity<Object> reindex(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/investigation/reindex",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, "reindex"),
                () -> investigationIndexingService.startReindex(context));
    }

    @GetMapping("/reindex/workflows")
    List<OperatorWorkflowStatus> reindexWorkflows(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return operatorWorkflowQueryService.list(
                accessContextResolver.resolveWorkspace(workspaceSlug, actorKey),
                OperatorWorkflowService.INVESTIGATION_REINDEX);
    }

    @GetMapping("/reindex/jobs/{jobId}/workflow")
    OperatorWorkflowStatus reindexWorkflow(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID jobId) {
        return operatorWorkflowQueryService.findByReference(
                accessContextResolver.resolveWorkspace(workspaceSlug, actorKey),
                OperatorWorkflowService.INVESTIGATION_REINDEX,
                OperatorWorkflowService.INVESTIGATION_REINDEX_JOB,
                jobId);
    }

    private static <T> ResponseEntity<T> withFreshness(T body, RegionReadFreshness freshness) {
        return ResponseEntity.ok()
                .header("X-Kay-Ledger-Read-Source", freshness.readSource())
                .header("X-Kay-Ledger-Source-Region", freshness.sourceRegion())
                .header("X-Kay-Ledger-Local-Region", freshness.localRegion())
                .header("X-Kay-Ledger-Replication-Lag-Millis", Long.toString(freshness.lagMillis()))
                .body(body);
    }
}
