package com.kayledger.api.shared.messaging.model;

import java.time.Instant;
import java.util.UUID;

public record InboxMessage(
        UUID id,
        UUID workspaceId,
        String topic,
        int partitionId,
        String messageKey,
        UUID eventId,
        String dedupeKey,
        String consumerName,
        Instant processedAt,
        String outcome,
        Instant createdAt) {
}
