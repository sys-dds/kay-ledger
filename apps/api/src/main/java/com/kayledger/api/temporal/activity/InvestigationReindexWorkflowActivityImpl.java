package com.kayledger.api.temporal.activity;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.investigation.store.InvestigationStore.ReindexJob;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;

import io.temporal.worker.Worker;

@Component
public class InvestigationReindexWorkflowActivityImpl implements InvestigationReindexWorkflowActivities, TemporalWorkerCustomizer {

    private final InvestigationIndexingService investigationIndexingService;

    public InvestigationReindexWorkflowActivityImpl(InvestigationIndexingService investigationIndexingService) {
        this.investigationIndexingService = investigationIndexingService;
    }

    @Override
    public InvestigationReindexWorkflowResult reindexWorkspace(UUID workspaceId, UUID reindexJobId) {
        ReindexJob job = investigationIndexingService.executeReindexForWorkflow(workspaceId, reindexJobId);
        return new InvestigationReindexWorkflowResult(job.indexedCount(), job.failedCount());
    }

    @Override
    public void customize(Worker worker) {
        worker.registerActivitiesImplementations(this);
    }
}
