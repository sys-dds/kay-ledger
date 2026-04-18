package com.kayledger.api.temporal.activity;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;

import io.temporal.worker.Worker;

@Component
public class ReconciliationWorkflowActivityImpl implements ReconciliationWorkflowActivities, TemporalWorkerCustomizer {

    private final ReconciliationService reconciliationService;

    public ReconciliationWorkflowActivityImpl(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @Override
    public ReconciliationWorkflowResult runReconciliation(UUID workspaceId, UUID reconciliationRunId) {
        ReconciliationRun run = reconciliationService.executeRunForWorkflow(workspaceId, reconciliationRunId);
        return new ReconciliationWorkflowResult(run.mismatchCount());
    }

    @Override
    public void customize(Worker worker) {
        worker.registerActivitiesImplementations(this);
    }
}
