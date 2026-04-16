package com.kayledger.api.identity.api;

import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.identity.Actor;
import com.kayledger.api.identity.ActorStore;
import com.kayledger.api.identity.CustomerProfile;
import com.kayledger.api.identity.ProfileStore;
import com.kayledger.api.identity.ProviderProfile;
import com.kayledger.api.identity.WorkspaceMembership;
import com.kayledger.api.identity.WorkspaceMembershipStore;
import com.kayledger.api.identity.application.AccessContext;
import com.kayledger.api.identity.application.AccessContextResolver;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.WorkspaceStore;

@RestController
@RequestMapping("/api")
public class WorkspaceActorsController {

    private final ActorStore actorStore;
    private final WorkspaceStore workspaceStore;
    private final WorkspaceMembershipStore membershipStore;
    private final ProfileStore profileStore;
    private final AccessContextResolver accessContextResolver;

    public WorkspaceActorsController(
            ActorStore actorStore,
            WorkspaceStore workspaceStore,
            WorkspaceMembershipStore membershipStore,
            ProfileStore profileStore,
            AccessContextResolver accessContextResolver) {
        this.actorStore = actorStore;
        this.workspaceStore = workspaceStore;
        this.membershipStore = membershipStore;
        this.profileStore = profileStore;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping("/actors")
    Actor createActor(@RequestBody CreateActorRequest request) {
        return actorStore.create(requireText(request.actorKey(), "actorKey"), requireText(request.displayName(), "displayName"));
    }

    @GetMapping("/actors")
    List<Actor> listActors(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN");
        return actorStore.listForWorkspace(context.workspaceId());
    }

    @PostMapping("/memberships")
    WorkspaceMembership createMembership(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateMembershipRequest request) {
        Workspace workspace = workspaceStore.findBySlug(requireText(request.workspaceSlug(), "workspaceSlug"))
                .orElseThrow(() -> new BadRequestException("workspaceSlug does not exist."));
        int existingMemberships = membershipStore.countForWorkspace(workspace.id());
        if (existingMemberships == 0) {
            if (!"OWNER".equals(request.role())) {
                throw new BadRequestException("First workspace membership must use OWNER role.");
            }
        } else {
            AccessContext context = resolve(workspaceSlug, actorKey);
            if (!context.workspaceId().equals(workspace.id())) {
                throw new BadRequestException("Membership can only be created inside the active workspace.");
            }
            context.requireAnyRole("OWNER", "ADMIN");
        }
        return membershipStore.create(workspace.id(), request.actorId(), requireRole(request.role()));
    }

    @GetMapping("/memberships")
    List<WorkspaceMembership> listMemberships(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN");
        return membershipStore.listForWorkspace(context.workspaceId());
    }

    @PostMapping("/provider-profiles")
    ProviderProfile createProviderProfile(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey,
            @RequestBody CreateProfileRequest request) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER");
        return profileStore.createProvider(context.workspaceId(), request.actorId(), requireText(request.displayName(), "displayName"));
    }

    @GetMapping("/provider-profiles")
    List<ProviderProfile> listProviderProfiles(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER", "CUSTOMER");
        return profileStore.listProviders(context.workspaceId());
    }

    @PostMapping("/customer-profiles")
    CustomerProfile createCustomerProfile(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey,
            @RequestBody CreateProfileRequest request) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "CUSTOMER");
        return profileStore.createCustomer(context.workspaceId(), request.actorId(), requireText(request.displayName(), "displayName"));
    }

    @GetMapping("/customer-profiles")
    List<CustomerProfile> listCustomerProfiles(
            @RequestHeader("X-Workspace-Slug") String workspaceSlug,
            @RequestHeader("X-Actor-Key") String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        context.requireAnyRole("OWNER", "ADMIN", "PROVIDER");
        return profileStore.listCustomers(context.workspaceId());
    }

    private AccessContext resolve(String workspaceSlug, String actorKey) {
        return accessContextResolver.resolve(workspaceSlug, actorKey);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireRole(String role) {
        String requiredRole = requireText(role, "role");
        if (!List.of("OWNER", "ADMIN", "PROVIDER", "CUSTOMER").contains(requiredRole)) {
            throw new BadRequestException("role is invalid.");
        }
        return requiredRole;
    }

    record CreateActorRequest(String actorKey, String displayName) {
    }

    record CreateMembershipRequest(String workspaceSlug, UUID actorId, String role) {
    }

    record CreateProfileRequest(UUID actorId, String displayName) {
    }
}
