package com.kayledger.api.temporal.activity;

import java.util.UUID;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ExportWorkflowActivities {

    @ActivityMethod
    ExportWorkflowResult generateExport(UUID workspaceId, UUID exportJobId);

    record ExportWorkflowResult(int rowCount) {
    }
}
