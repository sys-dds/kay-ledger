package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.Workflow;

import com.kayledger.api.temporal.activity.InvestigationReindexWorkflowActivities;
import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivities;

public class InvestigationReindexOperatorWorkflowImpl implements InvestigationReindexOperatorWorkflow {

    private final OperatorWorkflowStatusActivities statusActivities = Workflow.newActivityStub(
            OperatorWorkflowStatusActivities.class,
            OperatorWorkflowDefaults.activityOptions());
    private final InvestigationReindexWorkflowActivities reindexActivities = Workflow.newActivityStub(
            InvestigationReindexWorkflowActivities.class,
            OperatorWorkflowDefaults.activityOptions());

    @Override
    public void run(OperatorWorkflowInput input) {
        try {
            statusActivities.markRunning(input.workspaceId(), input.workflowId(), "Investigation reindex started.");
            InvestigationReindexWorkflowActivities.InvestigationReindexWorkflowResult result = reindexActivities.reindexWorkspace(input.workspaceId(), input.referenceId());
            int total = result.indexed() + result.failed();
            statusActivities.markSucceeded(input.workspaceId(), input.workflowId(), result.indexed(), total, "Investigation reindex completed.");
        } catch (RuntimeException exception) {
            statusActivities.markFailed(input.workspaceId(), input.workflowId(), exception.getMessage());
            throw exception;
        }
    }
}
