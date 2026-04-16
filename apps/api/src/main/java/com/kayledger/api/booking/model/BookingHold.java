package com.kayledger.api.booking.model;

import java.time.Instant;
import java.util.UUID;

public record BookingHold(
        UUID id,
        UUID workspaceId,
        UUID bookingId,
        String status,
        Instant expiresAt,
        String releaseReason,
        Instant createdAt,
        Instant updatedAt) {
}
