package com.kayledger.api.access.application;

import java.util.Set;

import org.springframework.stereotype.Component;

import com.kayledger.api.access.store.MembershipScopeStore;
import com.kayledger.api.access.store.PlatformRoleStore;
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
    private final MembershipScopeStore scopeStore;
    private final PlatformRoleStore platformRoleStore;

    public AccessContextResolver(
            WorkspaceStore workspaceStore,
            ActorStore actorStore,
            WorkspaceMembershipStore membershipStore,
            MembershipScopeStore scopeStore,
            PlatformRoleStore platformRoleStore) {
        this.workspaceStore = workspaceStore;
        this.actorStore = actorStore;
        this.membershipStore = membershipStore;
        this.scopeStore = scopeStore;
        this.platformRoleStore = platformRoleStore;
    }

    public AccessContext resolveWorkspace(String workspaceSlug, String actorKey) {
        Workspace workspace = workspaceStore.findBySlug(requireHeader(workspaceSlug, "X-Workspace-Slug"))
                .orElseThrow(() -> new NotFoundException("Workspace was not found."));
        Actor actor = actorStore.findByActorKey(requireHeader(actorKey, "X-Actor-Key"))
                .orElseThrow(() -> new NotFoundException("Actor was not found."));
        WorkspaceMembership membership = membershipStore.findActive(workspace.id(), actor.id())
                .orElseThrow(() -> new ForbiddenException("Actor is not an active member of this workspace."));

        return new AccessContext(
                workspace.id(),
                workspace.slug(),
                actor.id(),
                actor.actorKey(),
                membership.id(),
                membership.role(),
                Set.copyOf(scopeStore.listForMembership(membership.id())),
                Set.copyOf(platformRoleStore.listActiveRoles(actor.id())));
    }

    public OperatorContext resolveOperator(String actorKey) {
        Actor actor = actorStore.findByActorKey(requireHeader(actorKey, "X-Actor-Key"))
                .orElseThrow(() -> new NotFoundException("Actor was not found."));
        return new OperatorContext(
                actor.id(),
                actor.actorKey(),
                Set.copyOf(platformRoleStore.listActiveRoles(actor.id())));
    }

    private static String requireHeader(String value, String headerName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(headerName + " header is required.");
        }
        return value.trim();
    }
}
