package com.kayledger.api.evidence.api;

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
import com.kayledger.api.evidence.application.FinanceEvidenceService;
import com.kayledger.api.evidence.application.FinanceEvidenceService.EvidencePackDetails;
import com.kayledger.api.evidence.application.FinanceEvidenceService.GenerateEvidencePackCommand;
import com.kayledger.api.evidence.model.FinanceEvidenceExport;
import com.kayledger.api.evidence.model.FinanceEvidencePack;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/finance-evidence")
public class FinanceEvidenceController {

    private final AccessContextResolver accessContextResolver;
    private final FinanceEvidenceService financeEvidenceService;
    private final IdempotencyService idempotencyService;

    public FinanceEvidenceController(AccessContextResolver accessContextResolver, FinanceEvidenceService financeEvidenceService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.financeEvidenceService = financeEvidenceService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/packs")
    ResponseEntity<Object> generatePack(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody GenerateEvidencePackCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("evidence pack request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/finance-evidence/packs",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financeEvidenceService.generatePack(context, request));
    }

    @GetMapping("/packs")
    List<FinanceEvidencePack> listPacks(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return financeEvidenceService.listPacks(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/packs/{packId}")
    EvidencePackDetails packDetails(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID packId) {
        return financeEvidenceService.packDetails(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), packId);
    }

    @PostMapping("/packs/{packId}/exports")
    ResponseEntity<Object> generateExport(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID packId,
            @RequestBody GenerateExportRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/finance-evidence/packs/{packId}/exports",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, packId, request),
                () -> financeEvidenceService.generateExport(context, packId, request == null ? "JSON" : request.artifactFormat()));
    }

    @GetMapping("/packs/{packId}/exports")
    List<FinanceEvidenceExport> listExports(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID packId) {
        return financeEvidenceService.listExports(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), packId);
    }

    public record GenerateExportRequest(String artifactFormat) {
    }
}
