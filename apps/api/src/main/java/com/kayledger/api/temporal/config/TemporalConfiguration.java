package com.kayledger.api.temporal.config;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.SmartLifecycle;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

@Configuration
@ConditionalOnProperty(prefix = "kay-ledger.temporal", name = "enabled", havingValue = "true", matchIfMissing = true)
public class TemporalConfiguration {

    @Bean
    @ConditionalOnMissingBean
    WorkflowServiceStubs workflowServiceStubs(TemporalProperties properties) {
        return WorkflowServiceStubs.newServiceStubs(WorkflowServiceStubsOptions.newBuilder()
                .setTarget(properties.getTarget())
                .build());
    }

    @Bean
    @ConditionalOnMissingBean
    WorkflowClient workflowClient(WorkflowServiceStubs serviceStubs, TemporalProperties properties) {
        return WorkflowClient.newInstance(serviceStubs, WorkflowClientOptions.newBuilder()
                .setNamespace(properties.getNamespace())
                .build());
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerFactory workerFactory(WorkflowClient workflowClient) {
        return WorkerFactory.newInstance(workflowClient);
    }

    @Bean
    @ConditionalOnProperty(prefix = "kay-ledger.temporal", name = "worker-enabled", havingValue = "true", matchIfMissing = true)
    Worker operatorWorker(WorkerFactory workerFactory, TemporalProperties properties, List<TemporalWorkerCustomizer> customizers) {
        Worker worker = workerFactory.newWorker(properties.getTaskQueues().getOperatorWorkflows());
        customizers.forEach(customizer -> customizer.customize(worker));
        return worker;
    }

    @Bean
    @ConditionalOnProperty(prefix = "kay-ledger.temporal", name = "worker-enabled", havingValue = "true", matchIfMissing = true)
    SmartLifecycle temporalWorkerLifecycle(WorkerFactory workerFactory) {
        return new SmartLifecycle() {
            private boolean running;

            @Override
            public void start() {
                workerFactory.start();
                running = true;
            }

            @Override
            public void stop() {
                workerFactory.shutdown();
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }
        };
    }
}
