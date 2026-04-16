package com.kayledger.api.workspace.api;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.application.WorkspaceService;

@RestController
@RequestMapping("/api/workspaces")
public class WorkspacesController {

    private final WorkspaceService workspaceService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;

    public WorkspacesController(
            WorkspaceService workspaceService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService) {
        this.workspaceService = workspaceService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    ResponseEntity<Object> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateWorkspaceRequest request) {
        return idempotencyService.run(
                idempotencyKey,
                "GLOBAL",
                null,
                null,
                "POST /api/workspaces",
                IdempotencyService.fingerprint(request),
                () -> workspaceService.create(request.slug(), request.displayName()));
    }

    @GetMapping
    List<Workspace> list(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return workspaceService.listForContext(context);
    }

    record CreateWorkspaceRequest(String slug, String displayName) {
    }
}
