package com.kayledger.api.investigation.application;

import java.util.UUID;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.investigation.store.InvestigationStore.ReindexJob;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.shared.api.InternalFailureException;
import com.kayledger.api.temporal.application.OperatorWorkflowRecord;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.application.OperatorWorkflowStarter;
import com.kayledger.api.temporal.workflow.InvestigationReindexOperatorWorkflow;
import com.kayledger.api.temporal.workflow.OperatorWorkflowInput;

import io.temporal.client.WorkflowClient;

@Service
public class InvestigationIndexingService {

    private final InvestigationStore investigationStore;
    private final OpenSearchInvestigationClient openSearchClient;
    private final AccessPolicy accessPolicy;
    private final ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter;
    private final OperatorWorkflowService operatorWorkflowService;
    private final RegionService regionService;
    private final RegionReplicationService regionReplicationService;

    public InvestigationIndexingService(
            InvestigationStore investigationStore,
            OpenSearchInvestigationClient openSearchClient,
            AccessPolicy accessPolicy,
            ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter,
            OperatorWorkflowService operatorWorkflowService,
            RegionService regionService,
            RegionReplicationService regionReplicationService) {
        this.investigationStore = investigationStore;
        this.openSearchClient = openSearchClient;
        this.accessPolicy = accessPolicy;
        this.operatorWorkflowStarter = operatorWorkflowStarter;
        this.operatorWorkflowService = operatorWorkflowService;
        this.regionService = regionService;
        this.regionReplicationService = regionReplicationService;
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ReindexJob startReindex(AccessContext context) {
        requireReindexStart(context);
        ReindexJob job = investigationStore.createReindexJob(context.workspaceId(), context.actorId());
        OperatorWorkflowRecord workflow = null;
        try {
            workflow = operatorWorkflowService.createRequested(
                    context.workspaceId(),
                    OperatorWorkflowService.INVESTIGATION_REINDEX,
                    OperatorWorkflowService.INVESTIGATION_REINDEX_JOB,
                    job.id(),
                    OperatorWorkflowService.API,
                    context.actorId(),
                    1,
                    "Investigation reindex requested.");
            OperatorWorkflowStarter workflowStarter = operatorWorkflowStarter.getIfAvailable();
            if (workflowStarter == null) {
                throw new IllegalStateException("Temporal workflow starter bean is missing.");
            }
            ReindexJob requestedJob = job;
            OperatorWorkflowRecord requestedWorkflow = workflow;
            String temporalRunId = workflowStarter.start(workflow.workflowId(), (client, options) -> {
                InvestigationReindexOperatorWorkflow reindexWorkflow = client.newWorkflowStub(InvestigationReindexOperatorWorkflow.class, options);
                return WorkflowClient.start(reindexWorkflow::run, new OperatorWorkflowInput(context.workspaceId(), requestedJob.id(), requestedWorkflow.workflowId()));
            });
            operatorWorkflowService.attachRun(context.workspaceId(), workflow.workflowId(), temporalRunId);
            return investigationStore.attachReindexWorkflow(context.workspaceId(), job.id(), workflow.workflowId(), temporalRunId);
        } catch (Exception exception) {
            investigationStore.markReindexFailed(context.workspaceId(), job.id(), 0, 0, exception.getMessage());
            if (workflow != null) {
                operatorWorkflowService.markFailed(context.workspaceId(), workflow.workflowId(), exception.getMessage());
            }
            throw new InternalFailureException("Investigation reindex orchestration could not be started.", exception);
        }
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ReindexJob executeReindexForWorkflow(UUID workspaceId, UUID reindexJobId) {
        ReindexJob job = investigationStore.markReindexRunning(workspaceId, reindexJobId);
        try {
            markWorkflowProgress(workspaceId, job, 2, 3, "Loading source-of-truth investigation documents.");
            ReindexResult result = reindexWorkspace(workspaceId);
            markWorkflowProgress(workspaceId, job, 3, 3, "Persisting investigation index state.");
            if (result.failed() > 0) {
                investigationStore.markReindexFailed(workspaceId, reindexJobId, result.indexed(), result.failed(), "One or more documents failed to index.");
                throw new InternalFailureException("Investigation reindex completed with failed documents.", new IllegalStateException("failed documents: " + result.failed()));
            }
            return investigationStore.markReindexSucceeded(workspaceId, reindexJobId, result.indexed(), result.failed());
        } catch (InternalFailureException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            investigationStore.markReindexFailed(workspaceId, reindexJobId, 0, 0, exception.getMessage());
            throw new InternalFailureException("Investigation reindex failed; operator recovery is required.", exception);
        }
    }

    public ReindexResult reindexWorkspace(UUID workspaceId) {
        return indexDocuments(investigationStore.documentsForWorkspace(workspaceId));
    }

    public ReindexResult indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return indexDocuments(investigationStore.documentsForReference(workspaceId, referenceType, referenceId));
    }

    public ReindexResult replayRegionalSnapshot(UUID workspaceId, String referenceType, UUID referenceId, UUID recoveryActionId) {
        int replayed = 0;
        for (InvestigationDocument document : investigationStore.documentsForReference(workspaceId, referenceType, referenceId)) {
            regionReplicationService.publishInvestigationDocument(document, recoveryActionId);
            replayed++;
        }
        return new ReindexResult(replayed, 0);
    }

    private ReindexResult indexDocuments(Iterable<InvestigationDocument> documents) {
        int indexed = 0;
        int failed = 0;
        for (InvestigationDocument document : documents) {
            try {
                openSearchClient.index(document);
                investigationStore.recordIndexed(document);
                regionReplicationService.publishInvestigationDocument(document);
                indexed++;
            } catch (RuntimeException exception) {
                investigationStore.recordFailed(document, exception);
                failed++;
            }
        }
        if (indexed > 0) {
            openSearchClient.refresh();
        }
        return new ReindexResult(indexed, failed);
    }

    private void requireReindexStart(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
        regionService.requireOwnedForWrite(context, "investigation reindex start");
    }

    private void markWorkflowProgress(UUID workspaceId, ReindexJob job, int current, int total, String message) {
        if (job.temporalWorkflowId() != null) {
            operatorWorkflowService.markProgress(workspaceId, job.temporalWorkflowId(), current, total, message);
        }
    }

    public record ReindexResult(int indexed, int failed) {
    }
}
