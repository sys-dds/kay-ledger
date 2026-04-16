package com.kayledger.api.access.application;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.PlatformRole;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;

@Component
public class AccessPolicy {

    private static final Set<String> VALID_WORKSPACE_ROLES = Set.of(
            WorkspaceRole.OWNER,
            WorkspaceRole.ADMIN,
            WorkspaceRole.PROVIDER,
            WorkspaceRole.CUSTOMER);

    private static final Set<String> VALID_PLATFORM_ROLES = Set.of(PlatformRole.OPERATOR);

    private static final Set<String> VALID_SCOPES = Set.of(
            AccessScope.WORKSPACE_READ,
            AccessScope.ACTOR_READ,
            AccessScope.MEMBERSHIP_MANAGE,
            AccessScope.PROFILE_READ,
            AccessScope.PROFILE_MANAGE,
            AccessScope.ACCESS_CONTEXT_READ);

    public void requireWorkspaceRole(AccessContext context, String... roles) {
        if (!Set.copyOf(Arrays.asList(roles)).contains(context.workspaceRole())) {
            throw new ForbiddenException("Actor does not have the required workspace role.");
        }
    }

    public void requireScope(AccessContext context, String scope) {
        if (!context.workspaceScopes().contains(scope)) {
            throw new ForbiddenException("Actor does not have the required workspace scope.");
        }
    }

    public void requireOperator(OperatorContext context) {
        if (!context.platformRoles().contains(PlatformRole.OPERATOR)) {
            throw new ForbiddenException("Actor does not have the required platform role.");
        }
    }

    public void requireProviderCapableRole(String role) {
        if (!Set.of(WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.PROVIDER).contains(role)) {
            throw new ForbiddenException("Actor is not provider-capable in this workspace.");
        }
    }

    public void requireCustomerCapableRole(String role) {
        if (!Set.of(WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.CUSTOMER).contains(role)) {
            throw new ForbiddenException("Actor is not customer-capable in this workspace.");
        }
    }

    public String validateWorkspaceRole(String role) {
        String requiredRole = requireText(role, "role");
        if (!VALID_WORKSPACE_ROLES.contains(requiredRole)) {
            throw new BadRequestException("role is invalid.");
        }
        return requiredRole;
    }

    public String validatePlatformRole(String role) {
        String requiredRole = requireText(role, "platformRole");
        if (!VALID_PLATFORM_ROLES.contains(requiredRole)) {
            throw new BadRequestException("platformRole is invalid.");
        }
        return requiredRole;
    }

    public String validateScope(String scope) {
        String requiredScope = requireText(scope, "scope");
        if (!VALID_SCOPES.contains(requiredScope)) {
            throw new BadRequestException("scope is invalid.");
        }
        return requiredScope;
    }

    public List<String> defaultScopesForRole(String role) {
        return switch (role) {
            case WorkspaceRole.OWNER, WorkspaceRole.ADMIN -> List.of(
                    AccessScope.WORKSPACE_READ,
                    AccessScope.ACTOR_READ,
                    AccessScope.MEMBERSHIP_MANAGE,
                    AccessScope.PROFILE_READ,
                    AccessScope.PROFILE_MANAGE,
                    AccessScope.ACCESS_CONTEXT_READ);
            case WorkspaceRole.PROVIDER, WorkspaceRole.CUSTOMER -> List.of(
                    AccessScope.WORKSPACE_READ,
                    AccessScope.PROFILE_READ,
                    AccessScope.PROFILE_MANAGE,
                    AccessScope.ACCESS_CONTEXT_READ);
            default -> throw new BadRequestException("role is invalid.");
        };
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }
}
