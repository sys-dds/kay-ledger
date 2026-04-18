package com.kayledger.api.provider.api;

import java.util.List;

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
import com.kayledger.api.provider.application.ProviderCallbackService;
import com.kayledger.api.provider.application.ProviderCallbackService.CreateProviderConfigCommand;
import com.kayledger.api.provider.application.ProviderCallbackService.ProviderCallbackCommand;
import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.model.ProviderConfig;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/providers")
public class ProviderCallbacksController {

    private final AccessContextResolver accessContextResolver;
    private final ProviderCallbackService providerCallbackService;
    private final IdempotencyService idempotencyService;

    public ProviderCallbacksController(
            AccessContextResolver accessContextResolver,
            ProviderCallbackService providerCallbackService,
            IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.providerCallbackService = providerCallbackService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/configs")
    ResponseEntity<Object> createConfig(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateProviderConfigCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/providers/configs",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> providerCallbackService.createConfig(context, request));
    }

    @PostMapping("/{providerKey}/callbacks")
    ProviderCallback ingest(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Provider-Signature", required = false) String signature,
            @PathVariable String providerKey,
            @RequestBody ProviderCallbackCommand request) {
        return providerCallbackService.ingestExternal(workspaceSlug, providerKey, signature, request);
    }

    @GetMapping("/callbacks")
    List<ProviderCallback> listCallbacks(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return providerCallbackService.listCallbacks(context);
    }
}
