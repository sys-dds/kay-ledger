package com.kayledger.api.temporal.application;

import java.time.Duration;
import java.util.function.BiFunction;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import com.kayledger.api.temporal.config.TemporalProperties;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;

@Component
public class OperatorWorkflowStarter {

    private final ObjectProvider<WorkflowClient> workflowClient;
    private final TemporalProperties properties;

    public OperatorWorkflowStarter(ObjectProvider<WorkflowClient> workflowClient, TemporalProperties properties) {
        this.workflowClient = workflowClient;
        this.properties = properties;
    }

    public String start(String workflowId, BiFunction<WorkflowClient, WorkflowOptions, WorkflowExecution> starter) {
        WorkflowClient client = workflowClient.getIfAvailable();
        if (client == null) {
            throw new IllegalStateException("Temporal workflow client bean is missing.");
        }
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(properties.getTaskQueues().getOperatorWorkflows())
                .setWorkflowRunTimeout(Duration.ofSeconds(properties.getWorkflow().getStartToCloseTimeoutSeconds()))
                .build();
        return starter.apply(client, options).getRunId();
    }
}
