package com.kayledger.api.finance.model;

import java.time.Instant;
import java.util.UUID;

public record JournalEntry(
        UUID id,
        UUID workspaceId,
        String referenceType,
        UUID referenceId,
        UUID offeringId,
        UUID bookingId,
        String externalReference,
        String description,
        String status,
        Instant createdAt) {
}
