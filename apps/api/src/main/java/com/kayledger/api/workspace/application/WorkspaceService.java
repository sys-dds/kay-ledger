package com.kayledger.api.workspace.application;

import java.util.List;

import org.springframework.stereotype.Service;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.WorkspaceStore;

@Service
public class WorkspaceService {

    private final WorkspaceStore workspaceStore;
    private final AccessPolicy accessPolicy;

    public WorkspaceService(WorkspaceStore workspaceStore, AccessPolicy accessPolicy) {
        this.workspaceStore = workspaceStore;
        this.accessPolicy = accessPolicy;
    }

    public Workspace create(String slug, String displayName) {
        return workspaceStore.create(requireText(slug, "slug"), requireText(displayName, "displayName"));
    }

    public List<Workspace> listForContext(AccessContext context) {
        accessPolicy.requireScope(context, AccessScope.WORKSPACE_READ);
        return workspaceStore.findBySlug(context.workspaceSlug()).stream().toList();
    }

    public List<Workspace> listForOperator() {
        return workspaceStore.listAll();
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }
}
