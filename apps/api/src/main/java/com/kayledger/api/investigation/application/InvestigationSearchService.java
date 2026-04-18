package com.kayledger.api.investigation.application;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.model.InvestigationSearchHit;

@Service
public class InvestigationSearchService {

    private final OpenSearchInvestigationClient openSearchClient;
    private final InvestigationIndexingService indexingService;
    private final AccessPolicy accessPolicy;

    public InvestigationSearchService(OpenSearchInvestigationClient openSearchClient, InvestigationIndexingService indexingService, AccessPolicy accessPolicy) {
        this.openSearchClient = openSearchClient;
        this.indexingService = indexingService;
        this.accessPolicy = accessPolicy;
    }

    public InvestigationIndexingService.ReindexResult reindex(AccessContext context) {
        requireRead(context);
        return indexingService.reindexWorkspace(context.workspaceId());
    }

    public List<InvestigationSearchHit> search(AccessContext context, SearchCommand command) {
        requireRead(context);
        Map<String, Object> bool = new LinkedHashMap<>();
        List<Map<String, Object>> filters = new ArrayList<>();
        filters.add(term("workspaceId", context.workspaceId().toString()));
        if (command != null) {
            addTerm(filters, "paymentIntentId", command.paymentId());
            addTerm(filters, "refundId", command.refundId());
            addTerm(filters, "payoutRequestId", command.payoutId());
            addTerm(filters, "disputeId", command.disputeId());
            addTerm(filters, "providerEventId", command.providerEventId());
            addTerm(filters, "externalReference", command.externalReference());
            addTerm(filters, "businessReferenceId", command.businessReferenceId());
            addTerm(filters, "subscriptionId", command.subscriptionId());
            addTerm(filters, "providerProfileId", command.providerProfileId());
            addTerm(filters, "referenceId", command.referenceId());
        }
        bool.put("filter", filters);
        Map<String, Object> query = Map.of(
                "size", 50,
                "query", Map.of("bool", bool),
                "sort", List.of(Map.of("occurredAt", Map.of("order", "desc", "unmapped_type", "date"))));
        return openSearchClient.search(query);
    }

    public List<InvestigationSearchHit> byReference(AccessContext context, String referenceId) {
        return search(context, new SearchCommand(null, null, null, null, null, null, null, null, null, referenceId));
    }

    public List<InvestigationSearchHit> byProviderEvent(AccessContext context, String providerEventId) {
        return search(context, new SearchCommand(null, null, null, null, providerEventId, null, null, null, null, null));
    }

    public List<InvestigationSearchHit> byExternalReference(AccessContext context, String externalReference) {
        return search(context, new SearchCommand(null, null, null, null, null, externalReference, null, null, null, null));
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private static void addTerm(List<Map<String, Object>> filters, String field, String value) {
        if (value != null && !value.isBlank()) {
            filters.add(term(field, value.trim()));
        }
    }

    private static Map<String, Object> term(String field, String value) {
        return Map.of("term", Map.of(field, value));
    }

    public record SearchCommand(
            String paymentId,
            String refundId,
            String payoutId,
            String disputeId,
            String providerEventId,
            String externalReference,
            String businessReferenceId,
            String subscriptionId,
            String providerProfileId,
            String referenceId) {
    }
}
