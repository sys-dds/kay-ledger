package com.kayledger.api.investigation.api;

import java.util.List;

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
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/investigation")
public class InvestigationController {

    private final AccessContextResolver accessContextResolver;
    private final InvestigationSearchService investigationSearchService;
    private final IdempotencyService idempotencyService;

    public InvestigationController(AccessContextResolver accessContextResolver, InvestigationSearchService investigationSearchService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.investigationSearchService = investigationSearchService;
        this.idempotencyService = idempotencyService;
    }

    @GetMapping("/search")
    List<InvestigationSearchHit> search(
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
        return investigationSearchService.search(context, new SearchCommand(paymentId, refundId, payoutId, disputeId, providerEventId, externalReference, businessReferenceId, subscriptionId, providerProfileId, referenceId));
    }

    @GetMapping("/references/{referenceId}")
    List<InvestigationSearchHit> byReference(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String referenceId) {
        return investigationSearchService.byReference(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), referenceId);
    }

    @GetMapping("/provider-events/{providerEventId}")
    List<InvestigationSearchHit> byProviderEvent(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String providerEventId) {
        return investigationSearchService.byProviderEvent(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), providerEventId);
    }

    @GetMapping("/external-references/{externalReference}")
    List<InvestigationSearchHit> byExternalReference(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String externalReference) {
        return investigationSearchService.byExternalReference(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), externalReference);
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
                () -> investigationSearchService.reindex(context));
    }
}
