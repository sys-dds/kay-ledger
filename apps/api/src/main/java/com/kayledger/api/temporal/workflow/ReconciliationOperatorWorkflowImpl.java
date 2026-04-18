package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.Workflow;

import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivities;
import com.kayledger.api.temporal.activity.ReconciliationWorkflowActivities;

public class ReconciliationOperatorWorkflowImpl implements ReconciliationOperatorWorkflow {

    private final OperatorWorkflowStatusActivities statusActivities = Workflow.newActivityStub(
            OperatorWorkflowStatusActivities.class,
            OperatorWorkflowDefaults.activityOptions());
    private final ReconciliationWorkflowActivities reconciliationActivities = Workflow.newActivityStub(
            ReconciliationWorkflowActivities.class,
            OperatorWorkflowDefaults.activityOptions());

    @Override
    public void run(OperatorWorkflowInput input) {
        try {
            statusActivities.markRunning(input.workspaceId(), input.workflowId(), "Reconciliation run started.");
            statusActivities.markProgress(input.workspaceId(), input.workflowId(), 1, 4, "Dispatching reconciliation scan activity.");
            reconciliationActivities.runReconciliation(input.workspaceId(), input.referenceId());
            statusActivities.markSucceeded(input.workspaceId(), input.workflowId(), "Reconciliation run completed.");
        } catch (RuntimeException exception) {
            statusActivities.markFailed(input.workspaceId(), input.workflowId(), exception.getMessage());
            throw exception;
        }
    }
}
