package com.kayledger.api.workspace.api;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.application.WorkspaceService;

@RestController
@RequestMapping("/api/workspaces")
public class WorkspacesController {

    private final WorkspaceService workspaceService;
    private final AccessContextResolver accessContextResolver;

    public WorkspacesController(WorkspaceService workspaceService, AccessContextResolver accessContextResolver) {
        this.workspaceService = workspaceService;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping
    Workspace create(@RequestBody CreateWorkspaceRequest request) {
        return workspaceService.create(request.slug(), request.displayName());
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
