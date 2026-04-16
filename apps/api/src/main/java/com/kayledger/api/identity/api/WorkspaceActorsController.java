package com.kayledger.api.identity.api;

import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.identity.Actor;
import com.kayledger.api.identity.CustomerProfile;
import com.kayledger.api.identity.ProviderProfile;
import com.kayledger.api.identity.WorkspaceMembership;
import com.kayledger.api.identity.application.IdentityService;
import com.kayledger.api.shared.api.BadRequestException;

@RestController
@RequestMapping("/api")
public class WorkspaceActorsController {

    private final IdentityService identityService;
    private final AccessContextResolver accessContextResolver;

    public WorkspaceActorsController(
            IdentityService identityService,
            AccessContextResolver accessContextResolver) {
        this.identityService = identityService;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping("/actors")
    Actor createActor(@RequestBody CreateActorRequest request) {
        if (request.platformRoles() != null
                || request.platformRole() != null
                || request.operatorRole() != null
                || request.platformRolesSnake() != null
                || request.platformRoleSnake() != null
                || request.operatorRoleSnake() != null) {
            throw new BadRequestException("Platform/operator role assignment is not allowed on public actor creation.");
        }
        return identityService.createActor(request.actorKey(), request.displayName());
    }

    @GetMapping("/actors")
    List<Actor> listActors(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.listActors(context);
    }

    @PostMapping("/memberships")
    WorkspaceMembership createMembership(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateMembershipRequest request) {
        AccessContext context = workspaceSlug == null || actorKey == null ? null : resolve(workspaceSlug, actorKey);
        return identityService.createMembership(context, request.workspaceSlug(), request.actorId(), request.role(), request.scopes());
    }

    @GetMapping("/memberships")
    List<WorkspaceMembership> listMemberships(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.listMemberships(context);
    }

    @PostMapping("/provider-profiles")
    ProviderProfile createProviderProfile(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateProfileRequest request) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.createProviderProfile(context, request.actorId(), request.displayName());
    }

    @GetMapping("/provider-profiles")
    List<ProviderProfile> listProviderProfiles(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.listProviderProfiles(context);
    }

    @PostMapping("/customer-profiles")
    CustomerProfile createCustomerProfile(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateProfileRequest request) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.createCustomerProfile(context, request.actorId(), request.displayName());
    }

    @GetMapping("/customer-profiles")
    List<CustomerProfile> listCustomerProfiles(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = resolve(workspaceSlug, actorKey);
        return identityService.listCustomerProfiles(context);
    }

    private AccessContext resolve(String workspaceSlug, String actorKey) {
        return accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
    }

    record CreateActorRequest(
            String actorKey,
            String displayName,
            Object platformRoles,
            Object platformRole,
            Object operatorRole,
            @JsonProperty("platform_roles") Object platformRolesSnake,
            @JsonProperty("platform_role") Object platformRoleSnake,
            @JsonProperty("operator_role") Object operatorRoleSnake) {
    }

    record CreateMembershipRequest(String workspaceSlug, UUID actorId, String role, List<String> scopes) {
    }

    record CreateProfileRequest(UUID actorId, String displayName) {
    }
}
