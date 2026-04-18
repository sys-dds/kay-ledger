package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.Workflow;

import com.kayledger.api.temporal.activity.ExportWorkflowActivities;
import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivities;

public class ExportOperatorWorkflowImpl implements ExportOperatorWorkflow {

    private final OperatorWorkflowStatusActivities statusActivities = Workflow.newActivityStub(
            OperatorWorkflowStatusActivities.class,
            OperatorWorkflowDefaults.activityOptions());
    private final ExportWorkflowActivities exportActivities = Workflow.newActivityStub(
            ExportWorkflowActivities.class,
            OperatorWorkflowDefaults.activityOptions());

    @Override
    public void run(OperatorWorkflowInput input) {
        try {
            statusActivities.markRunning(input.workspaceId(), input.workflowId(), "Export generation started.");
            ExportWorkflowActivities.ExportWorkflowResult result = exportActivities.generateExport(input.workspaceId(), input.referenceId());
            statusActivities.markSucceeded(input.workspaceId(), input.workflowId(), result.rowCount(), result.rowCount(), "Export generation completed.");
        } catch (RuntimeException exception) {
            statusActivities.markFailed(input.workspaceId(), input.workflowId(), exception.getMessage());
            throw exception;
        }
    }
}
