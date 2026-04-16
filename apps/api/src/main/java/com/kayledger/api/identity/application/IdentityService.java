package com.kayledger.api.identity.application;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.PlatformRole;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.access.store.MembershipScopeStore;
import com.kayledger.api.access.store.PlatformRoleStore;
import com.kayledger.api.identity.Actor;
import com.kayledger.api.identity.ActorStore;
import com.kayledger.api.identity.CustomerProfile;
import com.kayledger.api.identity.ProfileStore;
import com.kayledger.api.identity.ProviderProfile;
import com.kayledger.api.identity.WorkspaceMembership;
import com.kayledger.api.identity.WorkspaceMembershipStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.WorkspaceStore;

@Service
public class IdentityService {

    private final ActorStore actorStore;
    private final WorkspaceStore workspaceStore;
    private final WorkspaceMembershipStore membershipStore;
    private final ProfileStore profileStore;
    private final PlatformRoleStore platformRoleStore;
    private final MembershipScopeStore scopeStore;
    private final AccessPolicy accessPolicy;

    public IdentityService(
            ActorStore actorStore,
            WorkspaceStore workspaceStore,
            WorkspaceMembershipStore membershipStore,
            ProfileStore profileStore,
            PlatformRoleStore platformRoleStore,
            MembershipScopeStore scopeStore,
            AccessPolicy accessPolicy) {
        this.actorStore = actorStore;
        this.workspaceStore = workspaceStore;
        this.membershipStore = membershipStore;
        this.profileStore = profileStore;
        this.platformRoleStore = platformRoleStore;
        this.scopeStore = scopeStore;
        this.accessPolicy = accessPolicy;
    }

    public Actor createActor(String actorKey, String displayName, List<String> platformRoles) {
        Actor actor = actorStore.create(requireText(actorKey, "actorKey"), requireText(displayName, "displayName"));
        for (String role : platformRoles == null ? List.<String>of() : platformRoles) {
            platformRoleStore.create(actor.id(), accessPolicy.validatePlatformRole(role));
        }
        return actor;
    }

    public List<Actor> listActors(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.ACTOR_READ);
        return actorStore.listForWorkspace(context.workspaceId());
    }

    public WorkspaceMembership createMembership(
            AccessContext context,
            String workspaceSlug,
            UUID actorId,
            String role,
            List<String> scopes) {
        Workspace workspace = workspaceStore.findBySlug(requireText(workspaceSlug, "workspaceSlug"))
                .orElseThrow(() -> new BadRequestException("workspaceSlug does not exist."));
        String workspaceRole = accessPolicy.validateWorkspaceRole(role);
        int existingMemberships = membershipStore.countForWorkspace(workspace.id());
        if (existingMemberships == 0) {
            if (!WorkspaceRole.OWNER.equals(workspaceRole)) {
                throw new BadRequestException("First workspace membership must use OWNER role.");
            }
        } else {
            if (context == null) {
                throw new BadRequestException("Workspace headers are required after owner bootstrap.");
            }
            if (!context.workspaceId().equals(workspace.id())) {
                throw new ForbiddenException("Membership can only be created inside the active workspace.");
            }
            accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
            accessPolicy.requireScope(context, AccessScope.MEMBERSHIP_MANAGE);
        }

        WorkspaceMembership membership = membershipStore.create(workspace.id(), actorId, workspaceRole);
        List<String> durableScopes = scopes == null || scopes.isEmpty()
                ? accessPolicy.defaultScopesForRole(workspaceRole)
                : scopes.stream().map(accessPolicy::validateScope).toList();
        durableScopes.forEach(scope -> scopeStore.create(membership.id(), scope));
        return membership;
    }

    public List<WorkspaceMembership> listMemberships(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.MEMBERSHIP_MANAGE);
        return membershipStore.listForWorkspace(context.workspaceId());
    }

    public ProviderProfile createProviderProfile(AccessContext context, UUID actorId, String displayName) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.PROVIDER);
        accessPolicy.requireScope(context, AccessScope.PROFILE_MANAGE);
        WorkspaceMembership targetMembership = membershipStore.findActiveByActor(context.workspaceId(), actorId)
                .orElseThrow(() -> new ForbiddenException("Actor is not an active member of this workspace."));
        accessPolicy.requireProviderCapableRole(targetMembership.role());
        if (WorkspaceRole.PROVIDER.equals(context.workspaceRole()) && !context.actorId().equals(actorId)) {
            throw new ForbiddenException("Providers can only create their own provider profile.");
        }
        return profileStore.createProvider(context.workspaceId(), actorId, requireText(displayName, "displayName"));
    }

    public List<ProviderProfile> listProviderProfiles(AccessContext context) {
        accessPolicy.requireScope(context, AccessScope.PROFILE_READ);
        return profileStore.listProviders(context.workspaceId());
    }

    public CustomerProfile createCustomerProfile(AccessContext context, UUID actorId, String displayName) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.CUSTOMER);
        accessPolicy.requireScope(context, AccessScope.PROFILE_MANAGE);
        WorkspaceMembership targetMembership = membershipStore.findActiveByActor(context.workspaceId(), actorId)
                .orElseThrow(() -> new ForbiddenException("Actor is not an active member of this workspace."));
        accessPolicy.requireCustomerCapableRole(targetMembership.role());
        if (WorkspaceRole.CUSTOMER.equals(context.workspaceRole()) && !context.actorId().equals(actorId)) {
            throw new ForbiddenException("Customers can only create their own customer profile.");
        }
        return profileStore.createCustomer(context.workspaceId(), actorId, requireText(displayName, "displayName"));
    }

    public List<CustomerProfile> listCustomerProfiles(AccessContext context) {
        accessPolicy.requireScope(context, AccessScope.PROFILE_READ);
        return profileStore.listCustomers(context.workspaceId());
    }

    public Set<String> listOperatorRoles(UUID actorId) {
        return Set.copyOf(platformRoleStore.listActiveRoles(actorId));
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }
}
