package com.kayledger.api.catalog;

import java.util.List;

public record OfferingDetails(
        Offering offering,
        List<OfferingPricingRule> pricingRules,
        List<OfferingAvailabilityWindow> availabilityWindows) {
}
