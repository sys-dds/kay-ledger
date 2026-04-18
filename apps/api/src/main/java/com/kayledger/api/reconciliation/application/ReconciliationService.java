package com.kayledger.api.reconciliation.application;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.PayoutRequest;
import com.kayledger.api.payment.model.RefundRecord;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.store.ProviderStore;
import com.kayledger.api.reconciliation.model.ReconciliationMismatch;
import com.kayledger.api.reconciliation.model.ReconciliationRun;
import com.kayledger.api.reconciliation.store.ReconciliationStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class ReconciliationService {

    private final ReconciliationStore reconciliationStore;
    private final ProviderStore providerStore;
    private final PaymentStore paymentStore;
    private final AccessPolicy accessPolicy;

    public ReconciliationService(ReconciliationStore reconciliationStore, ProviderStore providerStore, PaymentStore paymentStore, AccessPolicy accessPolicy) {
        this.reconciliationStore = reconciliationStore;
        this.providerStore = providerStore;
        this.paymentStore = paymentStore;
        this.accessPolicy = accessPolicy;
    }

    @Transactional
    public ReconciliationRun run(AccessContext context, RunReconciliationCommand command) {
        requireWrite(context);
        String runType = command == null || command.runType() == null ? "FULL" : command.runType();
        ReconciliationRun run = reconciliationStore.createRun(context.workspaceId(), runType);
        for (ProviderCallback callback : providerStore.listCallbacks(context.workspaceId())) {
            if (!"APPLIED".equals(callback.processingStatus())) {
                continue;
            }
            String providerState = providerState(callback.callbackType());
            String internalState = internalState(context.workspaceId(), callback);
            if (!providerState.equals(internalState)) {
                reconciliationStore.createMismatch(
                        context.workspaceId(),
                        run.id(),
                        callback.id(),
                        callback.businessReferenceType(),
                        callback.businessReferenceId(),
                        "STATE_MISMATCH",
                        internalState,
                        providerState,
                        "APPLY_PROVIDER_STATE");
            }
        }
        return reconciliationStore.completeRun(context.workspaceId(), run.id());
    }

    public List<ReconciliationRun> listRuns(AccessContext context) {
        requireRead(context);
        return reconciliationStore.listRuns(context.workspaceId());
    }

    public List<ReconciliationMismatch> listMismatches(AccessContext context) {
        requireRead(context);
        return reconciliationStore.listMismatches(context.workspaceId());
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
        if (!"APPLY_PROVIDER_STATE".equals(mismatch.suggestedAction())) {
            throw new BadRequestException("Mismatch does not have a safe automatic repair action.");
        }
        applyProviderState(context.workspaceId(), mismatch);
        return reconciliationStore.markApplied(context.workspaceId(), mismatch.id(), command == null ? "Applied provider state." : command.note());
    }

    private void applyProviderState(UUID workspaceId, ReconciliationMismatch mismatch) {
        switch (mismatch.businessReferenceType()) {
            case "PAYMENT_INTENT" -> paymentStore.applyProviderPaymentStatus(workspaceId, mismatch.businessReferenceId(), mismatch.providerState(), 0);
            case "PAYOUT_REQUEST" -> paymentStore.applyProviderPayoutStatus(workspaceId, mismatch.businessReferenceId(), mismatch.providerState(), null);
            case "REFUND" -> paymentStore.applyProviderRefundStatus(workspaceId, mismatch.businessReferenceId(), mismatch.providerState());
            default -> throw new BadRequestException("Unsupported mismatch reference type.");
        }
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
    }

    public record RunReconciliationCommand(String runType) {
    }

    public record RepairCommand(String note) {
    }
}
