package com.kayledger.api.risk.application;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.risk.model.RiskDecision;
import com.kayledger.api.risk.model.RiskFlag;
import com.kayledger.api.risk.model.RiskReview;
import com.kayledger.api.risk.store.RiskStore;
import com.kayledger.api.shared.api.BadRequestException;

@Service
public class RiskService {

    private static final long SUSPICIOUS_PAYOUT_THRESHOLD_MINOR = 100_000L;

    private final RiskStore riskStore;
    private final AccessPolicy accessPolicy;
    private final InvestigationIndexingService investigationIndexingService;

    public RiskService(RiskStore riskStore, AccessPolicy accessPolicy, InvestigationIndexingService investigationIndexingService) {
        this.riskStore = riskStore;
        this.accessPolicy = accessPolicy;
        this.investigationIndexingService = investigationIndexingService;
    }

    @Transactional
    public void evaluatePaymentFailure(UUID workspaceId, UUID providerProfileId) {
        int failed = riskStore.failedPaymentCountForProvider(workspaceId, providerProfileId);
        if (failed >= 3) {
            flag(workspaceId, "PROVIDER_PROFILE", providerProfileId, "REPEATED_FAILED_PAYMENT_BURST", "HIGH",
                    "Provider has " + failed + " failed payment intents in the last day.", failed);
        }
    }

    @Transactional
    public void evaluateRefundVelocity(UUID workspaceId, UUID providerProfileId) {
        int refunds = riskStore.refundCountForProvider(workspaceId, providerProfileId);
        if (refunds >= 3) {
            flag(workspaceId, "PROVIDER_PROFILE", providerProfileId, "REFUND_VELOCITY_SPIKE", "MEDIUM",
                    "Provider has " + refunds + " refunds or reversals in the last day.", refunds);
        }
    }

    @Transactional
    public void evaluatePayoutRequest(UUID workspaceId, UUID payoutRequestId, long amountMinor) {
        if (amountMinor >= SUSPICIOUS_PAYOUT_THRESHOLD_MINOR) {
            flag(workspaceId, "PAYOUT_REQUEST", payoutRequestId, "SUSPICIOUS_PAYOUT_THRESHOLD", "HIGH",
                    "Payout request is at or above the suspicious payout threshold.", 1);
        }
    }

    @Transactional
    public void evaluateMismatchBurst(UUID workspaceId) {
        int mismatches = riskStore.openMismatchCount(workspaceId);
        if (mismatches >= 3) {
            flag(workspaceId, "WORKSPACE", workspaceId, "RECONCILIATION_MISMATCH_BURST", "MEDIUM",
                    "Workspace has " + mismatches + " open reconciliation mismatches in the last day.", mismatches);
        }
    }

    public void requireNotBlocked(UUID workspaceId, String referenceType, UUID referenceId) {
        riskStore.latestBlockingDecision(workspaceId, referenceType, referenceId)
                .ifPresent(decision -> {
                    throw new BadRequestException(referenceType + " is blocked by a risk decision.");
                });
    }

    public List<RiskFlag> listFlags(AccessContext context) {
        requireRead(context);
        return riskStore.listFlags(context.workspaceId());
    }

    public List<RiskReview> listReviewQueue(AccessContext context) {
        requireRead(context);
        return riskStore.listReviewQueue(context.workspaceId());
    }

    @Transactional
    public RiskReview markInReview(AccessContext context, UUID reviewId) {
        requireWrite(context);
        RiskReview review = riskStore.markInReview(context.workspaceId(), reviewId, context.actorId());
        indexReference(context.workspaceId(), "RISK_REVIEW", review.id());
        indexReference(context.workspaceId(), "RISK_FLAG", review.riskFlagId());
        return review;
    }

    @Transactional
    public RiskDecision decide(AccessContext context, UUID reviewId, DecisionCommand command) {
        requireWrite(context);
        String outcome = requireOutcome(command == null ? null : command.outcome());
        String reason = requireText(command == null ? null : command.reason(), "reason");
        RiskDecision decision = riskStore.decide(context.workspaceId(), reviewId, outcome, reason, context.actorId());
        indexReference(context.workspaceId(), "RISK_DECISION", decision.id());
        indexReference(context.workspaceId(), "RISK_FLAG", decision.riskFlagId());
        return decision;
    }

    public List<RiskDecision> listDecisions(AccessContext context) {
        requireRead(context);
        return riskStore.listDecisions(context.workspaceId());
    }

    private RiskFlag flag(UUID workspaceId, String referenceType, UUID referenceId, String ruleCode, String severity, String reason, int signalCount) {
        RiskFlag flag = riskStore.upsertFlag(workspaceId, referenceType, referenceId, ruleCode, severity, reason, signalCount);
        RiskReview review = riskStore.ensureReview(workspaceId, flag.id());
        indexReference(workspaceId, "RISK_FLAG", flag.id());
        indexReference(workspaceId, "RISK_REVIEW", review.id());
        return flag;
    }

    private void indexReference(UUID workspaceId, String referenceType, UUID referenceId) {
        try {
            investigationIndexingService.indexReference(workspaceId, referenceType, referenceId);
        } catch (RuntimeException ignored) {
            // PostgreSQL remains the risk source of truth; operator search can be re-driven.
        }
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private static String requireOutcome(String outcome) {
        String value = requireText(outcome, "outcome");
        if (!List.of("ALLOW", "REVIEW", "BLOCK").contains(value)) {
            throw new BadRequestException("outcome is invalid.");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    public record DecisionCommand(String outcome, String reason) {
    }
}
