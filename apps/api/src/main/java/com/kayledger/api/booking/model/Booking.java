package com.kayledger.api.booking.model;

import java.time.Instant;
import java.util.UUID;

public record Booking(
        UUID id,
        UUID workspaceId,
        UUID offeringId,
        UUID providerProfileId,
        UUID customerProfileId,
        String offerType,
        String status,
        Instant scheduledStartAt,
        Instant scheduledEndAt,
        int quantityReserved,
        Instant holdExpiresAt,
        Instant confirmedAt,
        Instant cancelledAt,
        Instant createdAt,
        Instant updatedAt) {
}
