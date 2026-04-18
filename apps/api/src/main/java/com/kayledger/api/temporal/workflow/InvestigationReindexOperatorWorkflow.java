package com.kayledger.api.temporal.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface InvestigationReindexOperatorWorkflow {

    @WorkflowMethod
    void run(OperatorWorkflowInput input);
}
