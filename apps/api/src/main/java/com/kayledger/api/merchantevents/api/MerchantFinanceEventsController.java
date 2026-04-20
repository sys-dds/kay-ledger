package com.kayledger.api.merchantevents.api;

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
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService.ConfigureEndpointCommand;
import com.kayledger.api.merchantevents.model.MerchantFinanceEndpoint;
import com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/merchant-finance-events")
public class MerchantFinanceEventsController {

    private final AccessContextResolver accessContextResolver;
    private final MerchantFinanceEventService merchantFinanceEventService;
    private final IdempotencyService idempotencyService;

    public MerchantFinanceEventsController(AccessContextResolver accessContextResolver, MerchantFinanceEventService merchantFinanceEventService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.merchantFinanceEventService = merchantFinanceEventService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/endpoints")
    ResponseEntity<Object> configureEndpoint(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody ConfigureEndpointCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("merchant finance endpoint request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/merchant-finance-events/endpoints",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> merchantFinanceEventService.configureEndpoint(context, request));
    }

    @GetMapping("/endpoints")
    List<MerchantFinanceEndpoint> listEndpoints(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return merchantFinanceEventService.listEndpoints(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/deliveries")
    List<MerchantFinanceEventDelivery> listDeliveries(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return merchantFinanceEventService.listDeliveries(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/deliveries/{deliveryId}/redrive")
    ResponseEntity<Object> redrive(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID deliveryId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/merchant-finance-events/deliveries/{deliveryId}/redrive",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, deliveryId),
                () -> merchantFinanceEventService.redriveDelivery(context, deliveryId));
    }
}
