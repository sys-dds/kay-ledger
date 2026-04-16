package com.kayledger.api.catalog.api;

import java.util.UUID;

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

@RestController
@RequestMapping("/api/offerings")
public class OfferingsController {

    private final CatalogService catalogService;
    private final AccessContextResolver accessContextResolver;

    public OfferingsController(
            CatalogService catalogService,
            AccessContextResolver accessContextResolver) {
        this.catalogService = catalogService;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping
    OfferingDetails create(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateOfferingCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return catalogService.createDraft(context, request);
    }

    @GetMapping
    java.util.List<OfferingDetails> list(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return catalogService.list(context);
    }

    @PostMapping("/{offeringId}/publish")
    OfferingDetails publish(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID offeringId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return catalogService.publish(context, offeringId);
    }

    @PostMapping("/{offeringId}/archive")
    OfferingDetails archive(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID offeringId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return catalogService.archive(context, offeringId);
    }
}
