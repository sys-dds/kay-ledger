package com.kayledger.api.investigation.application;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.investigation.store.InvestigationStore;

@Service
public class InvestigationIndexingService {

    private final InvestigationStore investigationStore;
    private final OpenSearchInvestigationClient openSearchClient;

    public InvestigationIndexingService(InvestigationStore investigationStore, OpenSearchInvestigationClient openSearchClient) {
        this.investigationStore = investigationStore;
        this.openSearchClient = openSearchClient;
    }

    public ReindexResult reindexWorkspace(UUID workspaceId) {
        return indexDocuments(investigationStore.documentsForWorkspace(workspaceId));
    }

    public ReindexResult indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return indexDocuments(investigationStore.documentsForReference(workspaceId, referenceType, referenceId));
    }

    private ReindexResult indexDocuments(Iterable<InvestigationDocument> documents) {
        int indexed = 0;
        int failed = 0;
        for (InvestigationDocument document : documents) {
            try {
                openSearchClient.index(document);
                investigationStore.recordIndexed(document);
                indexed++;
            } catch (RuntimeException exception) {
                investigationStore.recordFailed(document, exception);
                failed++;
            }
        }
        return new ReindexResult(indexed, failed);
    }

    public record ReindexResult(int indexed, int failed) {
    }
}
