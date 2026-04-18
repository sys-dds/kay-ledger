package com.kayledger.api.temporal.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kay-ledger.temporal")
public class TemporalProperties {

    private boolean enabled = true;
    private String target = "localhost:7233";
    private String namespace = "default";
    private boolean workerEnabled = true;
    private final TaskQueues taskQueues = new TaskQueues();
    private final Workflow workflow = new Workflow();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isWorkerEnabled() {
        return workerEnabled;
    }

    public void setWorkerEnabled(boolean workerEnabled) {
        this.workerEnabled = workerEnabled;
    }

    public TaskQueues getTaskQueues() {
        return taskQueues;
    }

    public Workflow getWorkflow() {
        return workflow;
    }

    public static class TaskQueues {
        private String operatorWorkflows = "kay-ledger-operator-workflows";

        public String getOperatorWorkflows() {
            return operatorWorkflows;
        }

        public void setOperatorWorkflows(String operatorWorkflows) {
            this.operatorWorkflows = operatorWorkflows;
        }
    }

    public static class Workflow {
        private int startToCloseTimeoutSeconds = 3600;
        private int activityStartToCloseTimeoutSeconds = 300;
        private int retryInitialIntervalSeconds = 2;
        private int retryMaximumIntervalSeconds = 30;
        private int retryMaximumAttempts = 5;

        public int getStartToCloseTimeoutSeconds() {
            return startToCloseTimeoutSeconds;
        }

        public void setStartToCloseTimeoutSeconds(int startToCloseTimeoutSeconds) {
            this.startToCloseTimeoutSeconds = startToCloseTimeoutSeconds;
        }

        public int getActivityStartToCloseTimeoutSeconds() {
            return activityStartToCloseTimeoutSeconds;
        }

        public void setActivityStartToCloseTimeoutSeconds(int activityStartToCloseTimeoutSeconds) {
            this.activityStartToCloseTimeoutSeconds = activityStartToCloseTimeoutSeconds;
        }

        public int getRetryInitialIntervalSeconds() {
            return retryInitialIntervalSeconds;
        }

        public void setRetryInitialIntervalSeconds(int retryInitialIntervalSeconds) {
            this.retryInitialIntervalSeconds = retryInitialIntervalSeconds;
        }

        public int getRetryMaximumIntervalSeconds() {
            return retryMaximumIntervalSeconds;
        }

        public void setRetryMaximumIntervalSeconds(int retryMaximumIntervalSeconds) {
            this.retryMaximumIntervalSeconds = retryMaximumIntervalSeconds;
        }

        public int getRetryMaximumAttempts() {
            return retryMaximumAttempts;
        }

        public void setRetryMaximumAttempts(int retryMaximumAttempts) {
            this.retryMaximumAttempts = retryMaximumAttempts;
        }
    }
}
