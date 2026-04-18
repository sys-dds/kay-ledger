package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.Workflow;
import io.temporal.activity.ActivityOptions;

import com.kayledger.api.temporal.activity.InvestigationReindexWorkflowActivities;
import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivities;

public class InvestigationReindexOperatorWorkflowImpl implements InvestigationReindexOperatorWorkflow {

    private final OperatorWorkflowStatusActivities statusActivities;
    private final InvestigationReindexWorkflowActivities reindexActivities;

    public InvestigationReindexOperatorWorkflowImpl(ActivityOptions operatorActivityOptions) {
        this.statusActivities = Workflow.newActivityStub(
                OperatorWorkflowStatusActivities.class,
                operatorActivityOptions);
        this.reindexActivities = Workflow.newActivityStub(
                InvestigationReindexWorkflowActivities.class,
                operatorActivityOptions);
    }

    @Override
    public void run(OperatorWorkflowInput input) {
        try {
            statusActivities.markRunning(input.workspaceId(), input.workflowId(), "Investigation reindex started.");
            statusActivities.markProgress(input.workspaceId(), input.workflowId(), 1, 3, "Dispatching investigation reindex activity.");
            reindexActivities.reindexWorkspace(input.workspaceId(), input.referenceId());
            statusActivities.markSucceeded(input.workspaceId(), input.workflowId(), "Investigation reindex completed.");
        } catch (RuntimeException exception) {
            statusActivities.markFailed(input.workspaceId(), input.workflowId(), exception.getMessage());
            throw exception;
        }
    }
}
