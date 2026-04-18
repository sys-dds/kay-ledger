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
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reconciliation.application.ReconciliationService.RepairCommand;
import com.kayledger.api.reconciliation.application.ReconciliationService.RunReconciliationCommand;
import com.kayledger.api.reconciliation.model.ReconciliationMismatch;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/reconciliation")
public class ReconciliationController {

    private final AccessContextResolver accessContextResolver;
    private final ReconciliationService reconciliationService;
    private final IdempotencyService idempotencyService;

    public ReconciliationController(
            AccessContextResolver accessContextResolver,
            ReconciliationService reconciliationService,
            IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.reconciliationService = reconciliationService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/runs")
    ResponseEntity<Object> run(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) RunReconciliationCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/runs",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> reconciliationService.run(context, request));
    }

    @GetMapping("/runs")
    List<ReconciliationRun> listRuns(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return reconciliationService.listRuns(context);
    }

    @GetMapping("/mismatches")
    List<ReconciliationMismatch> listMismatches(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return reconciliationService.listMismatches(context);
    }

    @PostMapping("/mismatches/{mismatchId}/mark-repair")
    ResponseEntity<Object> markRepair(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID mismatchId,
            @RequestBody(required = false) RepairCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/mismatches/{mismatchId}/mark-repair",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, mismatchId, request),
                () -> reconciliationService.markRepair(context, mismatchId, request));
    }

    @PostMapping("/mismatches/{mismatchId}/apply-repair")
    ResponseEntity<Object> applyRepair(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID mismatchId,
            @RequestBody(required = false) RepairCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/reconciliation/mismatches/{mismatchId}/apply-repair",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, mismatchId, request),
                () -> reconciliationService.applyRepair(context, mismatchId, request));
    }
}
