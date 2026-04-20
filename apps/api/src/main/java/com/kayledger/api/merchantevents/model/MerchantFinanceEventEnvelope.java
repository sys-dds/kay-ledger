package com.kayledger.api.merchantevents.model;

import java.time.Instant;
import java.util.UUID;

public record MerchantFinanceEventEnvelope(
        int envelopeVersion,
        UUID eventId,
        UUID deliveryId,
        String eventType,
        String sourceReferenceType,
        UUID sourceReferenceId,
        String eventKey,
        Instant occurredAt,
        Instant attemptedAt,
        Object payload) {
}
