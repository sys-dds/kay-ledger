package com.kayledger.api.temporal.activity;

import java.util.UUID;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface OperatorWorkflowStatusActivities {

    @ActivityMethod
    void markRunning(UUID workspaceId, String workflowId, String progressMessage);

    @ActivityMethod
    void markProgress(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage);

    @ActivityMethod
    void markSucceeded(UUID workspaceId, String workflowId, String progressMessage);

    @ActivityMethod
    void markFailed(UUID workspaceId, String workflowId, String failureReason);
}
