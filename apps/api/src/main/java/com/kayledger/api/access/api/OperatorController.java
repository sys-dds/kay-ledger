package com.kayledger.api.access.api;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.application.OperatorContext;
import com.kayledger.api.identity.application.IdentityService;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.application.WorkspaceService;

@RestController
@RequestMapping("/api/operator")
public class OperatorController {

    private final AccessContextResolver accessContextResolver;
    private final AccessPolicy accessPolicy;
    private final IdentityService identityService;
    private final WorkspaceService workspaceService;
    private final IdempotencyService idempotencyService;
    private final String bootstrapOperatorAdminKey;

    public OperatorController(
            AccessContextResolver accessContextResolver,
            AccessPolicy accessPolicy,
            IdentityService identityService,
            WorkspaceService workspaceService,
            IdempotencyService idempotencyService,
            @Value("${kay-ledger.bootstrap.operator-admin-key:}") String bootstrapOperatorAdminKey) {
        this.accessContextResolver = accessContextResolver;
        this.accessPolicy = accessPolicy;
        this.identityService = identityService;
        this.workspaceService = workspaceService;
        this.idempotencyService = idempotencyService;
        this.bootstrapOperatorAdminKey = bootstrapOperatorAdminKey;
    }

    @PostMapping("/bootstrap/operator-role")
    ResponseEntity<Object> grantOperatorRole(
            @RequestHeader(value = "X-Bootstrap-Key", required = false) String bootstrapKey,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        if (bootstrapOperatorAdminKey.isBlank() || bootstrapKey == null || !bootstrapOperatorAdminKey.equals(bootstrapKey)) {
            throw new ForbiddenException("Bootstrap operator role assignment is not allowed.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "GLOBAL",
                null,
                null,
                "POST /api/operator/bootstrap/operator-role",
                IdempotencyService.fingerprint(actorKey),
                () -> {
                    identityService.grantOperatorRole(actorKey);
                    return Map.of("status", "OK");
                });
    }

    @GetMapping("/workspaces")
    List<Workspace> listWorkspaces(@RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        OperatorContext context = accessContextResolver.resolveOperator(actorKey);
        accessPolicy.requireOperator(context);
        return workspaceService.listForOperator();
    }
}
