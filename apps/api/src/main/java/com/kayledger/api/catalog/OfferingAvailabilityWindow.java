package com.kayledger.api.catalog;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

public record OfferingAvailabilityWindow(
        UUID id,
        UUID workspaceId,
        UUID offeringId,
        int weekday,
        LocalTime startLocalTime,
        LocalTime endLocalTime,
        LocalDate effectiveFrom,
        LocalDate effectiveTo,
        String status,
        Instant createdAt,
        Instant updatedAt) {
}
