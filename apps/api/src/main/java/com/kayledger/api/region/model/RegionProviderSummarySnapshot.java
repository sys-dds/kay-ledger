package com.kayledger.api.region.model;

import java.time.Instant;
import java.util.UUID;

import com.kayledger.api.reporting.model.ProviderFinancialSummary;

public record RegionProviderSummarySnapshot(
        ProviderFinancialSummary summary,
        String sourceRegion,
        String targetRegion,
        UUID replicationEventId,
        Instant replicatedAt) {
}
