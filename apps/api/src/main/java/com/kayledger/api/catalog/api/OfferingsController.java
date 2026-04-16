package com.kayledger.api.catalog.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.catalog.Offering;
import com.kayledger.api.catalog.OfferingStore;
import com.kayledger.api.identity.ProfileStore;
import com.kayledger.api.identity.application.AccessContext;
import com.kayledger.api.identity.application.AccessContextResolver;
import com.kayledger.api.shared.api.BadRequestException;

@RestController
@RequestMapping("/api/offerings")
public class OfferingsController {

    private final OfferingStore offeringStore;
    private final ProfileStore profileStore;
    private final AccessContextResolver accessContextResolver;
    private final ObjectMapper objectMapper;

    public OfferingsController(
            OfferingStore offeringStore,
            ProfileStore profileStore,
            AccessContextResolver accessContextResolver,
            ObjectMapper objectMapper) {
        this.offeringStore = offeringStore;
        this.profileStore = profileStore;
        this.accessContextResolver = accessContextResolver;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    Offering create(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey,
            @RequestBody CreateOfferingRequest request) {
        AccessContext context = accessContextResolver.resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER");
        profileStore.findProvider(context.workspaceId(), request.providerProfileId())
                .orElseThrow(() -> new BadRequestException("providerProfileId is not valid for this workspace."));
        return offeringStore.create(
                context.workspaceId(),
                request.providerProfileId(),
                requireText(request.title(), "title"),
                json(request.pricingMetadata()),
                request.durationMinutes(),
                json(request.schedulingMetadata()));
    }

    @GetMapping
    List<Offering> list(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = accessContextResolver.resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER", "CUSTOMER");
        return offeringStore.listForWorkspace(context.workspaceId());
    }

    private String json(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("metadata must be valid JSON.");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    record CreateOfferingRequest(
            UUID providerProfileId,
            String title,
            Map<String, Object> pricingMetadata,
            Integer durationMinutes,
            Map<String, Object> schedulingMetadata) {
    }
}
