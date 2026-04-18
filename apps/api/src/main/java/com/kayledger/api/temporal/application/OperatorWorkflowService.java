package com.kayledger.api.temporal.application;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OperatorWorkflowService {

    public static final String EXPORT = "EXPORT";
    public static final String RECONCILIATION = "RECONCILIATION";
    public static final String INVESTIGATION_REINDEX = "INVESTIGATION_REINDEX";
    public static final String EXPORT_JOB = "EXPORT_JOB";
    public static final String RECONCILIATION_RUN = "RECONCILIATION_RUN";
    public static final String INVESTIGATION_REINDEX_JOB = "INVESTIGATION_REINDEX_JOB";
    public static final String API = "API";

    private final OperatorWorkflowStore operatorWorkflowStore;

    public OperatorWorkflowService(OperatorWorkflowStore operatorWorkflowStore) {
        this.operatorWorkflowStore = operatorWorkflowStore;
    }

    @Transactional
    public OperatorWorkflowRecord createRequested(
            UUID workspaceId,
            String workflowType,
            String businessReferenceType,
            UUID businessReferenceId,
            String triggerMode,
            UUID requestedByActorId,
            int progressTotal,
            String progressMessage) {
        return operatorWorkflowStore.createRequested(
                workspaceId,
                workflowType,
                businessReferenceType,
                businessReferenceId,
                workflowId(workspaceId, workflowType, businessReferenceType, businessReferenceId),
                triggerMode == null ? API : triggerMode,
                requestedByActorId,
                progressTotal,
                progressMessage);
    }

    @Transactional
    public OperatorWorkflowRecord attachRun(UUID workspaceId, String workflowId, String runId) {
        return operatorWorkflowStore.attachRun(workspaceId, workflowId, runId);
    }

    @Transactional
    public OperatorWorkflowRecord markRunning(UUID workspaceId, String workflowId, String progressMessage) {
        return operatorWorkflowStore.markRunning(workspaceId, workflowId, progressMessage);
    }

    @Transactional
    public OperatorWorkflowRecord markProgress(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        return operatorWorkflowStore.markProgress(workspaceId, workflowId, progressCurrent, progressTotal, progressMessage);
    }

    @Transactional
    public OperatorWorkflowRecord markSucceeded(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        return operatorWorkflowStore.markSucceeded(workspaceId, workflowId, progressCurrent, progressTotal, progressMessage);
    }

    @Transactional
    public OperatorWorkflowRecord markFailed(UUID workspaceId, String workflowId, String failureReason) {
        return operatorWorkflowStore.markFailed(workspaceId, workflowId, failureReason);
    }

    public Optional<OperatorWorkflowRecord> find(UUID workspaceId, String workflowId) {
        return operatorWorkflowStore.find(workspaceId, workflowId);
    }

    public Optional<OperatorWorkflowRecord> findByReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return operatorWorkflowStore.findByReference(workspaceId, referenceType, referenceId);
    }

    public List<OperatorWorkflowRecord> list(UUID workspaceId, String workflowType) {
        return operatorWorkflowStore.list(workspaceId, workflowType);
    }

    public static String workflowId(UUID workspaceId, String workflowType, String businessReferenceType, UUID businessReferenceId) {
        return "kay-ledger:" + workspaceId + ":" + workflowType.toLowerCase() + ":" + businessReferenceType.toLowerCase() + ":" + businessReferenceId;
    }
}
