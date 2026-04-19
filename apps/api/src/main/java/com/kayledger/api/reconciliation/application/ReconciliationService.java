package com.kayledger.api.reconciliation.application;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.ObjectProvider;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.PayoutRequest;
import com.kayledger.api.payment.model.RefundRecord;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.store.ProviderStore;
import com.kayledger.api.reconciliation.model.ReconciliationMismatch;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.reconciliation.store.ReconciliationStore;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.risk.application.RiskService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.InternalFailureException;
import com.kayledger.api.shared.api.NotFoundException;
import com.kayledger.api.temporal.application.OperatorWorkflowRecord;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.application.OperatorWorkflowStarter;
import com.kayledger.api.temporal.workflow.OperatorWorkflowInput;
import com.kayledger.api.temporal.workflow.ReconciliationOperatorWorkflow;

import io.temporal.client.WorkflowClient;

@Service
public class ReconciliationService {

    private static final String STATE_MISMATCH = "STATE_MISMATCH";
    private static final String MISSING_INTERNAL_REFERENCE = "MISSING_INTERNAL_REFERENCE";
    private static final String APPLY_PROVIDER_STATE = "APPLY_PROVIDER_STATE";
    private static final String MANUAL_REVIEW = "MANUAL_REVIEW";

    private final ReconciliationStore reconciliationStore;
    private final ProviderStore providerStore;
    private final PaymentStore paymentStore;
    private final PaymentService paymentService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;
    private final RiskService riskService;
    private final ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter;
    private final OperatorWorkflowService operatorWorkflowService;
    private final RegionService regionService;

    public ReconciliationService(
            ReconciliationStore reconciliationStore,
            ProviderStore providerStore,
            PaymentStore paymentStore,
            PaymentService paymentService,
            AccessPolicy accessPolicy,
            ObjectMapper objectMapper,
            InvestigationIndexingService investigationIndexingService,
            RiskService riskService,
            ObjectProvider<OperatorWorkflowStarter> operatorWorkflowStarter,
            OperatorWorkflowService operatorWorkflowService,
            RegionService regionService) {
        this.reconciliationStore = reconciliationStore;
        this.providerStore = providerStore;
        this.paymentStore = paymentStore;
        this.paymentService = paymentService;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
        this.riskService = riskService;
        this.operatorWorkflowStarter = operatorWorkflowStarter;
        this.operatorWorkflowService = operatorWorkflowService;
        this.regionService = regionService;
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ReconciliationRun startRun(AccessContext context, RunReconciliationCommand command) {
        requireWrite(context);
        String runType = command == null || command.runType() == null ? "FULL" : command.runType();
        ReconciliationRun run = reconciliationStore.createRequestedRun(context.workspaceId(), runType);
        OperatorWorkflowRecord workflow = null;
        try {
            workflow = operatorWorkflowService.createRequested(
                    context.workspaceId(),
                    OperatorWorkflowService.RECONCILIATION,
                    OperatorWorkflowService.RECONCILIATION_RUN,
                    run.id(),
                    OperatorWorkflowService.API,
                    context.actorId(),
                    1,
                    "Reconciliation requested.");
            OperatorWorkflowStarter workflowStarter = operatorWorkflowStarter.getIfAvailable();
            if (workflowStarter == null) {
                throw new IllegalStateException("Temporal workflow starter bean is missing.");
            }
            ReconciliationRun requestedRun = run;
            OperatorWorkflowRecord requestedWorkflow = workflow;
            String temporalRunId = workflowStarter.start(workflow.workflowId(), (client, options) -> {
                ReconciliationOperatorWorkflow reconciliationWorkflow = client.newWorkflowStub(ReconciliationOperatorWorkflow.class, options);
                return WorkflowClient.start(reconciliationWorkflow::run, new OperatorWorkflowInput(context.workspaceId(), requestedRun.id(), requestedWorkflow.workflowId()));
            });
            operatorWorkflowService.attachRun(context.workspaceId(), workflow.workflowId(), temporalRunId);
            return reconciliationStore.attachWorkflow(context.workspaceId(), run.id(), workflow.workflowId(), temporalRunId);
        } catch (Exception exception) {
            reconciliationStore.markFailed(context.workspaceId(), run.id(), exception.getMessage());
            if (workflow != null) {
                operatorWorkflowService.markFailed(context.workspaceId(), workflow.workflowId(), exception.getMessage());
            }
            throw new InternalFailureException("Reconciliation orchestration could not be started.", exception);
        }
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ReconciliationRun run(AccessContext context, RunReconciliationCommand command) {
        requireWrite(context);
        String runType = command == null || command.runType() == null ? "FULL" : command.runType();
        ReconciliationRun run = reconciliationStore.createRun(context.workspaceId(), runType);
        return executeRun(context.workspaceId(), run.id());
    }

    @Transactional(noRollbackFor = InternalFailureException.class)
    public ReconciliationRun executeRunForWorkflow(UUID workspaceId, UUID runId) {
        return executeRun(workspaceId, runId);
    }

    private ReconciliationRun executeRun(UUID workspaceId, UUID runId) {
        ReconciliationRun run = reconciliationStore.markRunning(workspaceId, runId);
        try {
        markWorkflowProgress(workspaceId, run, 2, 4, "Scanning applied provider callbacks.");
        for (ProviderCallback callback : providerStore.listCallbacks(workspaceId)) {
            if (!"APPLIED".equals(callback.processingStatus())) {
                continue;
            }
            String providerState = providerState(callback.callbackType());
            String internalState = internalState(workspaceId, callback);
            if (!providerState.equals(internalState)) {
                String driftCategory = driftCategory(internalState);
                reconciliationStore.createMismatch(
                        workspaceId,
                        run.id(),
                        callback.id(),
                        callback.businessReferenceType(),
                        callback.businessReferenceId(),
                        driftCategory,
                        internalState,
                        providerState,
                        suggestedAction(driftCategory, callback.businessReferenceType()));
                continue;
            }
            Long providerAmount = providerAmount(callback);
            Long internalAmount = internalAmount(workspaceId, callback);
            if (providerAmount != null && internalAmount != null && !providerAmount.equals(internalAmount)) {
                reconciliationStore.createMismatch(
                        workspaceId,
                        run.id(),
                        callback.id(),
                        callback.businessReferenceType(),
                        callback.businessReferenceId(),
                        "AMOUNT_MISMATCH",
                        internalState + ":" + internalAmount,
                        providerState + ":" + providerAmount,
                        MANUAL_REVIEW);
            }
        }
        markWorkflowProgress(workspaceId, run, 3, 4, "Creating projection and journal drift mismatches.");
        reconciliationStore.createEntityCentricMismatches(workspaceId, run.id());
        markWorkflowProgress(workspaceId, run, 4, 4, "Evaluating risk follow-up for reconciliation mismatches.");
        riskService.evaluateMismatchBurst(workspaceId);
        int mismatchCount = reconciliationStore.countMismatches(workspaceId, run.id());
        ReconciliationRun completed = reconciliationStore.completeRun(workspaceId, run.id(), mismatchCount);
        reindexRun(workspaceId, completed.id());
        return completed;
        } catch (RuntimeException exception) {
            reconciliationStore.markFailed(workspaceId, run.id(), exception.getMessage());
            throw new InternalFailureException("Reconciliation run failed; operator review is required.", exception);
        }
    }

    public List<ReconciliationRun> listRuns(AccessContext context) {
        requireRead(context);
        return reconciliationStore.listRuns(context.workspaceId());
    }

    public List<ReconciliationMismatch> listMismatches(AccessContext context) {
        requireRead(context);
        return reconciliationStore.listMismatches(context.workspaceId());
    }

    public Map<String, Object> investigationReference(AccessContext context, UUID mismatchId) {
        requireRead(context);
        ReconciliationMismatch mismatch = reconciliationStore.findMismatch(context.workspaceId(), mismatchId)
                .orElseThrow(() -> new NotFoundException("Reconciliation mismatch was not found."));
        return Map.of(
                "referenceType", "RECONCILIATION_MISMATCH",
                "referenceId", mismatch.id(),
                "businessReferenceType", mismatch.businessReferenceType(),
                "businessReferenceId", mismatch.businessReferenceId(),
                "driftCategory", mismatch.driftCategory());
    }

    @Transactional
    public ReconciliationMismatch markRepair(AccessContext context, UUID mismatchId, RepairCommand command) {
        requireWrite(context);
        return reconciliationStore.markRepair(context.workspaceId(), mismatchId, command == null ? null : command.note());
    }

    @Transactional
    public ReconciliationMismatch applyRepair(AccessContext context, UUID mismatchId, RepairCommand command) {
        requireWrite(context);
        ReconciliationMismatch mismatch = reconciliationStore.findMismatch(context.workspaceId(), mismatchId)
                .orElseThrow(() -> new NotFoundException("Reconciliation mismatch was not found."));
        if (!STATE_MISMATCH.equals(mismatch.driftCategory()) || !APPLY_PROVIDER_STATE.equals(mismatch.suggestedAction())) {
            throw new BadRequestException("Mismatch does not have a safe automatic repair action.");
        }
        applyProviderState(context.workspaceId(), mismatch);
        ReconciliationMismatch applied = reconciliationStore.markApplied(context.workspaceId(), mismatch.id(), command == null ? "Applied provider state." : command.note());
        reindexMismatch(context.workspaceId(), applied);
        return applied;
    }

    private void reindexRun(UUID workspaceId, UUID runId) {
        try {
            reconciliationStore.listMismatches(workspaceId).stream()
                    .filter(mismatch -> runId.equals(mismatch.reconciliationRunId()))
                    .forEach(mismatch -> reindexMismatch(workspaceId, mismatch));
        } catch (RuntimeException ignored) {
            // Search indexing can be safely re-driven; reconciliation truth remains in PostgreSQL.
        }
    }

    private void reindexMismatch(UUID workspaceId, ReconciliationMismatch mismatch) {
        try {
            investigationIndexingService.indexReference(workspaceId, "RECONCILIATION_MISMATCH", mismatch.id());
            investigationIndexingService.indexReference(workspaceId, mismatch.businessReferenceType(), mismatch.businessReferenceId());
        } catch (RuntimeException ignored) {
            // Search indexing can be safely re-driven; reconciliation truth remains in PostgreSQL.
        }
    }

    private void markWorkflowProgress(UUID workspaceId, ReconciliationRun run, int current, int total, String message) {
        if (run.temporalWorkflowId() != null) {
            operatorWorkflowService.markProgress(workspaceId, run.temporalWorkflowId(), current, total, message);
        }
    }

    private void applyProviderState(UUID workspaceId, ReconciliationMismatch mismatch) {
        switch (mismatch.businessReferenceType()) {
            case "PAYMENT_INTENT" -> paymentService.applyProviderPaymentTruth(workspaceId, mismatch.businessReferenceId(), mismatch.providerState(), 0, "reconciliation:" + mismatch.id());
            case "PAYOUT_REQUEST" -> paymentService.applyProviderPayoutTruth(workspaceId, mismatch.businessReferenceId(), mismatch.providerState(), "reconciliation:" + mismatch.id(), "reconciliation provider-state repair");
            case "REFUND" -> paymentService.applyProviderRefundTruth(workspaceId, mismatch.businessReferenceId(), mismatch.providerState(), "reconciliation:" + mismatch.id(), "reconciliation provider-state repair");
            default -> throw new BadRequestException("Unsupported mismatch reference type.");
        }
    }

    private static String driftCategory(String internalState) {
        return "MISSING".equals(internalState) ? MISSING_INTERNAL_REFERENCE : STATE_MISMATCH;
    }

    private static String suggestedAction(String driftCategory, String referenceType) {
        if (!STATE_MISMATCH.equals(driftCategory)) {
            return MANUAL_REVIEW;
        }
        return switch (referenceType) {
            case "PAYMENT_INTENT", "PAYOUT_REQUEST", "REFUND" -> APPLY_PROVIDER_STATE;
            default -> MANUAL_REVIEW;
        };
    }

    private String internalState(UUID workspaceId, ProviderCallback callback) {
        return switch (callback.businessReferenceType()) {
            case "PAYMENT_INTENT" -> paymentStore.find(workspaceId, callback.businessReferenceId())
                    .map(PaymentIntent::status)
                    .orElse("MISSING");
            case "PAYOUT_REQUEST" -> paymentStore.findPayout(workspaceId, callback.businessReferenceId())
                    .map(PayoutRequest::status)
                    .orElse("MISSING");
            case "REFUND" -> paymentStore.findRefund(workspaceId, callback.businessReferenceId())
                    .map(RefundRecord::status)
                    .orElse("MISSING");
            default -> "MISSING";
        };
    }

    private Long internalAmount(UUID workspaceId, ProviderCallback callback) {
        return switch (callback.businessReferenceType()) {
            case "PAYMENT_INTENT" -> paymentStore.find(workspaceId, callback.businessReferenceId())
                    .map(intent -> switch (callback.callbackType()) {
                        case "PAYMENT_AUTHORIZED" -> intent.authorizedAmountMinor();
                        case "PAYMENT_CAPTURED" -> intent.capturedAmountMinor();
                        case "PAYMENT_SETTLED" -> intent.settledAmountMinor();
                        default -> intent.grossAmountMinor();
                    })
                    .orElse(null);
            case "PAYOUT_REQUEST" -> paymentStore.findPayout(workspaceId, callback.businessReferenceId())
                    .map(PayoutRequest::requestedAmountMinor)
                    .orElse(null);
            case "REFUND" -> paymentStore.findRefund(workspaceId, callback.businessReferenceId())
                    .map(RefundRecord::amountMinor)
                    .orElse(null);
            default -> null;
        };
    }

    private Long providerAmount(ProviderCallback callback) {
        try {
            Map<String, Object> payload = objectMapper.readValue(callback.payloadJson(), new TypeReference<>() {
            });
            Object amount = payload.get("amountMinor");
            if (amount == null) {
                return null;
            }
            return Long.valueOf(amount.toString());
        } catch (Exception exception) {
            return null;
        }
    }

    private String providerState(String callbackType) {
        return switch (callbackType) {
            case "PAYMENT_AUTHORIZED" -> "AUTHORIZED";
            case "PAYMENT_CAPTURED" -> "CAPTURED";
            case "PAYMENT_SETTLED" -> "SETTLED";
            case "PAYMENT_FAILED" -> "FAILED";
            case "REFUND_SUCCEEDED", "PAYOUT_SUCCEEDED" -> "SUCCEEDED";
            case "REFUND_FAILED", "PAYOUT_FAILED" -> "FAILED";
            default -> throw new BadRequestException("Unsupported callback type.");
        };
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
        regionService.requireOwnedForWrite(context, "reconciliation run start");
    }

    public record RunReconciliationCommand(String runType) {
    }

    public record RepairCommand(String note) {
    }
}
