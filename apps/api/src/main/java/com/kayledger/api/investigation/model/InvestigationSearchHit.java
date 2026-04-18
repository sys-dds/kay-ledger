package com.kayledger.api.investigation.model;

import java.util.Map;

public record InvestigationSearchHit(
        String documentId,
        String documentType,
        String referenceType,
        String referenceId,
        String status,
        Map<String, Object> source) {
}
