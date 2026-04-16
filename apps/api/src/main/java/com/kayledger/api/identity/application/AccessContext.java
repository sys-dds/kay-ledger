package com.kayledger.api.identity.application;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import com.kayledger.api.shared.api.ForbiddenException;

public record AccessContext(
        UUID workspaceId,
        String workspaceSlug,
        UUID actorId,
        String actorKey,
        String role) {

    public void requireAnyRole(String... allowedRoles) {
        Set<String> allowed = Set.copyOf(Arrays.asList(allowedRoles));
        if (!allowed.contains(role)) {
            throw new ForbiddenException("Actor does not have the required workspace role.");
        }
    }
}
