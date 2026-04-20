package com.kayledger.api.reconciliation.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.evidence.application.FinanceEvidenceService;
import com.kayledger.api.evidence.model.FinanceEvidencePack;
import com.kayledger.api.reconciliation.api.ReconciliationRequests.CreateReconciliationRunRequest;
import com.kayledger.api.reconciliation.api.ReconciliationRequests.ProviderTruthImportRequest;
import com.kayledger.api.reconciliation.api.ReconciliationRequests.ReopenReconciliationItemRequest;
import com.kayledger.api.reconciliation.api.ReconciliationRequests.ResolveReconciliationItemRequest;
import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reconciliation.model.ReconciliationItemAuditEvent;
import com.kayledger.api.reconciliation.model.ReconciliationItem;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/reconciliation")
public class ReconciliationController {

    private final AccessContextResolver accessContextResolver;
    private final ReconciliationService reconciliationService;
    private final FinanceEvidenceService financeEvidenceService;
    private final IdempotencyService idempotencyService;

    public ReconciliationController(
            AccessContextResolver accessContextResolver,
            ReconciliationService reconciliationService,
            FinanceEvidenceService financeEvidenceService,
            IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.reconciliationService = reconciliationService;
        this.financeEvidenceService = financeEvidenceService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/provider-truth-imports")
    ResponseEntity<Object> recordProviderTruth(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody ProviderTruthImportRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("provider truth import request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/provider-truth-imports",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> reconciliationService.recordProviderTruth(context, request.toTruth()));
    }

    @PostMapping("/runs")
    ResponseEntity<Object> createRun(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateReconciliationRunRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null || request.truthImportId() == null) {
            throw new BadRequestException("truthImportId is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/runs",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> reconciliationService.createRunFromProviderTruth(context, request.truthImportId()));
    }

    @GetMapping("/runs")
    List<ReconciliationRun> listRuns(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return reconciliationService.listRuns(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/runs/{runId}")
    ReconciliationRun runDetails(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID runId) {
        return reconciliationService.runDetails(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), runId);
    }

    @GetMapping("/runs/{runId}/items")
    List<ReconciliationItem> listRunItems(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID runId,
            @RequestParam(value = "unresolvedOnly", defaultValue = "false") boolean unresolvedOnly) {
        return reconciliationService.listItems(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), runId, unresolvedOnly);
    }

    @GetMapping("/runs/{runId}/evidence-packs")
    List<FinanceEvidencePack> runEvidencePacks(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID runId) {
        return financeEvidenceService.listPacksForSource(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), "PROVIDER_RECONCILIATION_RUN", runId);
    }

    @GetMapping("/items")
    List<ReconciliationItem> listItems(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestParam("runId") UUID runId,
            @RequestParam(value = "unresolvedOnly", defaultValue = "false") boolean unresolvedOnly) {
        return reconciliationService.listItems(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), runId, unresolvedOnly);
    }

    @GetMapping("/items/{itemId}/events")
    List<ReconciliationItemAuditEvent> listItemEvents(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID itemId) {
        return reconciliationService.listItemEvents(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), itemId);
    }

    @PostMapping("/items/{itemId}/resolve")
    ResponseEntity<Object> resolveItem(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID itemId,
            @RequestBody ResolveReconciliationItemRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("resolution request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/items/{itemId}/resolve",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, itemId, request),
                () -> reconciliationService.resolveItem(context, itemId, request.resolutionOutcome(), request.resolutionNote()));
    }

    @PostMapping("/items/{itemId}/reopen")
    ResponseEntity<Object> reopenItem(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID itemId,
            @RequestBody ReopenReconciliationItemRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("reopen request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/items/{itemId}/reopen",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, itemId, request),
                () -> reconciliationService.reopenItem(context, itemId, request.reason()));
    }
}
