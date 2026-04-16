package com.kayledger.api.identity.application;

import org.springframework.stereotype.Component;

import com.kayledger.api.identity.Actor;
import com.kayledger.api.identity.ActorStore;
import com.kayledger.api.identity.WorkspaceMembership;
import com.kayledger.api.identity.WorkspaceMembershipStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.api.NotFoundException;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.WorkspaceStore;

@Component
public class AccessContextResolver {

    private final WorkspaceStore workspaceStore;
    private final ActorStore actorStore;
    private final WorkspaceMembershipStore membershipStore;

    public AccessContextResolver(
            WorkspaceStore workspaceStore,
            ActorStore actorStore,
            WorkspaceMembershipStore membershipStore) {
        this.workspaceStore = workspaceStore;
        this.actorStore = actorStore;
        this.membershipStore = membershipStore;
    }

    public AccessContext resolve(String workspaceSlug, String actorKey) {
        String requiredWorkspaceSlug = requireHeader(workspaceSlug, "X-Workspace-Slug");
        String requiredActorKey = requireHeader(actorKey, "X-Actor-Key");

        Workspace workspace = workspaceStore.findBySlug(requiredWorkspaceSlug)
                .orElseThrow(() -> new NotFoundException("Workspace was not found."));
        Actor actor = actorStore.findByActorKey(requiredActorKey)
                .orElseThrow(() -> new NotFoundException("Actor was not found."));
        WorkspaceMembership membership = membershipStore.findActive(workspace.id(), actor.id())
                .orElseThrow(() -> new ForbiddenException("Actor is not an active member of this workspace."));

        return new AccessContext(
                workspace.id(),
                workspace.slug(),
                actor.id(),
                actor.actorKey(),
                membership.role());
    }

    private static String requireHeader(String value, String headerName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(headerName + " header is required.");
        }
        return value.trim();
    }
}
