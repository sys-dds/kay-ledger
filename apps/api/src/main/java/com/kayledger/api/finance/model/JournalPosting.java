package com.kayledger.api.finance.model;

import java.time.Instant;
import java.util.UUID;

public record JournalPosting(
        UUID id,
        UUID journalEntryId,
        UUID accountId,
        String entrySide,
        long amountMinor,
        String currencyCode,
        Instant createdAt) {
}
