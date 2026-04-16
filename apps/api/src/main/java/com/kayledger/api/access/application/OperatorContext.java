package com.kayledger.api.access.application;

import java.util.Set;
import java.util.UUID;

public record OperatorContext(
        UUID actorId,
        String actorKey,
        Set<String> platformRoles) {
}
