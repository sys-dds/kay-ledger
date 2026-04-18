package com.kayledger.api.temporal.workflow;

import java.util.UUID;

public record OperatorWorkflowInput(
        UUID workspaceId,
        UUID referenceId,
        String workflowId) {
}
