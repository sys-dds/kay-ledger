package com.kayledger.api.access.application;

import java.util.Set;
import java.util.UUID;

public record AccessContext(
        UUID workspaceId,
        String workspaceSlug,
        UUID actorId,
        String actorKey,
        UUID membershipId,
        String workspaceRole,
        Set<String> workspaceScopes,
        Set<String> platformRoles) {
}
