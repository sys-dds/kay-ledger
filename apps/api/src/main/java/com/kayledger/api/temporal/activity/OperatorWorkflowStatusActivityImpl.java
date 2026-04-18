package com.kayledger.api.temporal.activity;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.kayledger.api.temporal.application.OperatorWorkflowService;

@Component
public class OperatorWorkflowStatusActivityImpl implements OperatorWorkflowStatusActivities {

    private final OperatorWorkflowService operatorWorkflowService;

    public OperatorWorkflowStatusActivityImpl(OperatorWorkflowService operatorWorkflowService) {
        this.operatorWorkflowService = operatorWorkflowService;
    }

    @Override
    public void markRunning(UUID workspaceId, String workflowId, String progressMessage) {
        operatorWorkflowService.markRunning(workspaceId, workflowId, progressMessage);
    }

    @Override
    public void markProgress(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        operatorWorkflowService.markProgress(workspaceId, workflowId, progressCurrent, progressTotal, progressMessage);
    }

    @Override
    public void markSucceeded(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        operatorWorkflowService.markSucceeded(workspaceId, workflowId, progressCurrent, progressTotal, progressMessage);
    }

    @Override
    public void markFailed(UUID workspaceId, String workflowId, String failureReason) {
        operatorWorkflowService.markFailed(workspaceId, workflowId, failureReason);
    }
}
