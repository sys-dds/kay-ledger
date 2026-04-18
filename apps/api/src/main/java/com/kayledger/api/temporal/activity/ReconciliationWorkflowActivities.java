package com.kayledger.api.temporal.activity;

import java.util.UUID;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ReconciliationWorkflowActivities {

    @ActivityMethod
    ReconciliationWorkflowResult runReconciliation(UUID workspaceId, UUID reconciliationRunId);

    record ReconciliationWorkflowResult(int mismatchCount) {
    }
}
