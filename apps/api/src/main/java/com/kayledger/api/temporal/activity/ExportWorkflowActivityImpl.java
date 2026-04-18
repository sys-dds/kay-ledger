package com.kayledger.api.temporal.activity;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.reporting.model.ExportJob;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;

import io.temporal.worker.Worker;

@Component
public class ExportWorkflowActivityImpl implements ExportWorkflowActivities, TemporalWorkerCustomizer {

    private final ReportingService reportingService;

    public ExportWorkflowActivityImpl(ReportingService reportingService) {
        this.reportingService = reportingService;
    }

    @Override
    public ExportWorkflowResult generateExport(UUID workspaceId, UUID exportJobId) {
        ExportJob job = reportingService.generateExportForWorkflow(workspaceId, exportJobId);
        return new ExportWorkflowResult(job.rowCount());
    }

    @Override
    public void customize(Worker worker) {
        worker.registerActivitiesImplementations(this);
    }
}
