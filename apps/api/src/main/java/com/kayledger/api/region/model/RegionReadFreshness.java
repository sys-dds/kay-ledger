package com.kayledger.api.region.model;

import java.time.Instant;

public record RegionReadFreshness(
        String readSource,
        String sourceRegion,
        String localRegion,
        long lagMillis,
        Instant lastAppliedAt) {
}
