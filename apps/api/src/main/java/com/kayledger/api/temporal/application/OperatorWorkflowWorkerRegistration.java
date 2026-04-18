package com.kayledger.api.temporal.application;

import org.springframework.stereotype.Component;

import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivityImpl;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;
import com.kayledger.api.temporal.workflow.ExportOperatorWorkflowImpl;
import com.kayledger.api.temporal.workflow.InvestigationReindexOperatorWorkflowImpl;
import com.kayledger.api.temporal.workflow.ReconciliationOperatorWorkflowImpl;

import io.temporal.worker.Worker;

@Component
public class OperatorWorkflowWorkerRegistration implements TemporalWorkerCustomizer {

    private final OperatorWorkflowStatusActivityImpl statusActivity;

    public OperatorWorkflowWorkerRegistration(OperatorWorkflowStatusActivityImpl statusActivity) {
        this.statusActivity = statusActivity;
    }

    @Override
    public void customize(Worker worker) {
        worker.registerWorkflowImplementationTypes(
                ExportOperatorWorkflowImpl.class,
                ReconciliationOperatorWorkflowImpl.class,
                InvestigationReindexOperatorWorkflowImpl.class);
        worker.registerActivitiesImplementations(statusActivity);
    }
}
