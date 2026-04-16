package com.kayledger.api.workspace.api;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.identity.application.AccessContext;
import com.kayledger.api.identity.application.AccessContextResolver;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.WorkspaceStore;

@RestController
@RequestMapping("/api/workspaces")
public class WorkspacesController {

    private final WorkspaceStore workspaceStore;
    private final AccessContextResolver accessContextResolver;

    public WorkspacesController(WorkspaceStore workspaceStore, AccessContextResolver accessContextResolver) {
        this.workspaceStore = workspaceStore;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping
    Workspace create(@RequestBody CreateWorkspaceRequest request) {
        return workspaceStore.create(requireText(request.slug(), "slug"), requireText(request.displayName(), "displayName"));
    }

    @GetMapping
    List<Workspace> list(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = accessContextResolver.resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER", "CUSTOMER");
        return workspaceStore.findBySlug(context.workspaceSlug()).stream().toList();
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    record CreateWorkspaceRequest(String slug, String displayName) {
    }
}
