package com.kayledger.api.temporal.workflow;

import java.time.Duration;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;

final class OperatorWorkflowDefaults {

    private OperatorWorkflowDefaults() {
    }

    static ActivityOptions activityOptions() {
        return ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(5))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(2))
                        .setMaximumInterval(Duration.ofSeconds(30))
                        .setMaximumAttempts(5)
                        .build())
                .build();
    }
}
