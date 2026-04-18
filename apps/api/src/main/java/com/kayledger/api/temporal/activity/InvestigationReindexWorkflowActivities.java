package com.kayledger.api.temporal.activity;

import java.util.UUID;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface InvestigationReindexWorkflowActivities {

    @ActivityMethod
    InvestigationReindexWorkflowResult reindexWorkspace(UUID workspaceId, UUID reindexJobId);

    record InvestigationReindexWorkflowResult(int indexed, int failed) {
    }
}
