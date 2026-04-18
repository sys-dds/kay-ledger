package com.kayledger.api.temporal.application;

import org.springframework.stereotype.Component;

import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivityImpl;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;
import com.kayledger.api.temporal.workflow.ExportOperatorWorkflowImpl;
import com.kayledger.api.temporal.workflow.InvestigationReindexOperatorWorkflowImpl;
import com.kayledger.api.temporal.workflow.ReconciliationOperatorWorkflowImpl;

import io.temporal.activity.ActivityOptions;
import io.temporal.worker.Worker;

@Component
public class OperatorWorkflowWorkerRegistration implements TemporalWorkerCustomizer {

    private final OperatorWorkflowStatusActivityImpl statusActivity;
    private final ActivityOptions operatorActivityOptions;

    public OperatorWorkflowWorkerRegistration(
            OperatorWorkflowStatusActivityImpl statusActivity,
            ActivityOptions operatorActivityOptions) {
        this.statusActivity = statusActivity;
        this.operatorActivityOptions = operatorActivityOptions;
    }

    @Override
    public void customize(Worker worker) {
        worker.registerWorkflowImplementationFactory(
                com.kayledger.api.temporal.workflow.ExportOperatorWorkflow.class,
                () -> new ExportOperatorWorkflowImpl(operatorActivityOptions));
        worker.registerWorkflowImplementationFactory(
                com.kayledger.api.temporal.workflow.ReconciliationOperatorWorkflow.class,
                () -> new ReconciliationOperatorWorkflowImpl(operatorActivityOptions));
        worker.registerWorkflowImplementationFactory(
                com.kayledger.api.temporal.workflow.InvestigationReindexOperatorWorkflow.class,
                () -> new InvestigationReindexOperatorWorkflowImpl(operatorActivityOptions));
        worker.registerActivitiesImplementations(statusActivity);
    }
}
