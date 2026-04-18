package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.Workflow;
import io.temporal.activity.ActivityOptions;

import com.kayledger.api.temporal.activity.ExportWorkflowActivities;
import com.kayledger.api.temporal.activity.OperatorWorkflowStatusActivities;

public class ExportOperatorWorkflowImpl implements ExportOperatorWorkflow {

    private final OperatorWorkflowStatusActivities statusActivities;
    private final ExportWorkflowActivities exportActivities;

    public ExportOperatorWorkflowImpl(ActivityOptions operatorActivityOptions) {
        this.statusActivities = Workflow.newActivityStub(
                OperatorWorkflowStatusActivities.class,
                operatorActivityOptions);
        this.exportActivities = Workflow.newActivityStub(
                ExportWorkflowActivities.class,
                operatorActivityOptions);
    }

    @Override
    public void run(OperatorWorkflowInput input) {
        try {
            statusActivities.markRunning(input.workspaceId(), input.workflowId(), "Export generation started.");
            statusActivities.markProgress(input.workspaceId(), input.workflowId(), 1, 4, "Dispatching export generation activity.");
            exportActivities.generateExport(input.workspaceId(), input.referenceId());
            statusActivities.markSucceeded(input.workspaceId(), input.workflowId(), "Export generation completed.");
        } catch (RuntimeException exception) {
            statusActivities.markFailed(input.workspaceId(), input.workflowId(), exception.getMessage());
            throw exception;
        }
    }
}
