package com.kayledger.api.temporal.config;

import io.temporal.worker.Worker;

public interface TemporalWorkerCustomizer {

    void customize(Worker worker);
}
