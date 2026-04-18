package com.kayledger.api.temporal.config;

import java.time.Duration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;

@Configuration
public class OperatorWorkflowActivityOptionsConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "operatorActivityOptions")
    ActivityOptions operatorActivityOptions(TemporalProperties properties) {
        TemporalProperties.Workflow workflow = properties.getWorkflow();
        return ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofSeconds(workflow.getActivityStartToCloseTimeoutSeconds()))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(workflow.getRetryInitialIntervalSeconds()))
                        .setMaximumInterval(Duration.ofSeconds(workflow.getRetryMaximumIntervalSeconds()))
                        .setMaximumAttempts(workflow.getRetryMaximumAttempts())
                        .build())
                .build();
    }
}
