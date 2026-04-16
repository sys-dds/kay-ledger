package com.kayledger.api.catalog.api;

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
import com.kayledger.api.catalog.OfferingDetails;
import com.kayledger.api.catalog.application.CatalogService;
import com.kayledger.api.catalog.application.CatalogService.CreateOfferingCommand;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/offerings")
public class OfferingsController {

    private final CatalogService catalogService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;

    public OfferingsController(
            CatalogService catalogService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService) {
        this.catalogService = catalogService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    ResponseEntity<Object> create(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateOfferingCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/offerings",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> catalogService.createDraft(context, request));
    }

    @GetMapping
    java.util.List<OfferingDetails> list(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return catalogService.list(context);
    }

    @PostMapping("/{offeringId}/publish")
    ResponseEntity<Object> publish(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID offeringId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/offerings/{offeringId}/publish",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, offeringId),
                () -> catalogService.publish(context, offeringId));
    }

    @PostMapping("/{offeringId}/archive")
    ResponseEntity<Object> archive(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID offeringId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/offerings/{offeringId}/archive",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, offeringId),
                () -> catalogService.archive(context, offeringId));
    }
}
