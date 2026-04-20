package com.kayledger.api.close.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record AccountingPeriod(
        UUID id,
        UUID workspaceId,
        LocalDate periodStart,
        LocalDate periodEnd,
        String status,
        UUID openedByActorId,
        UUID closedByActorId,
        UUID reopenedByActorId,
        String closeReason,
        String reopenReason,
        Instant closedAt,
        Instant reopenedAt,
        Instant createdAt,
        Instant updatedAt) {
}
