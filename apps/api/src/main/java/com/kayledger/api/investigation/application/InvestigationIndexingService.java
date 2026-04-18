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
        int indexed = 0;
        int failed = 0;
        for (InvestigationDocument document : investigationStore.documentsForWorkspace(workspaceId)) {
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
