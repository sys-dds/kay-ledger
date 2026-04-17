package com.kayledger.api.payment.model;

import java.time.Instant;
import java.util.UUID;

public record PaymentIntent(
        UUID id,
        UUID workspaceId,
        UUID bookingId,
        UUID providerProfileId,
        String status,
        String currencyCode,
        long grossAmountMinor,
        long feeAmountMinor,
        long netAmountMinor,
        long authorizedAmountMinor,
        long capturedAmountMinor,
        long settledAmountMinor,
        String externalReference,
        Instant createdAt,
        Instant updatedAt) {
}
