package com.kayledger.api.approval.application;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.approval.model.FinancialApprovalDecision;
import com.kayledger.api.approval.model.FinancialApprovalExecutionState;
import com.kayledger.api.approval.model.FinancialApprovalRequest;
import com.kayledger.api.approval.store.FinancialApprovalStore;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
@EnableConfigurationProperties(FinancialApprovalProperties.class)
public class FinancialApprovalService {

    public static final String ACTION_FINANCIAL_PERIOD_CLOSE = "FINANCIAL_PERIOD_CLOSE";
    public static final String ACTION_FINANCIAL_PERIOD_REOPEN = "FINANCIAL_PERIOD_REOPEN";
    public static final String ACTION_PAYOUT_OPERATOR_SUCCESS = "PAYOUT_OPERATOR_SUCCESS";
    public static final String ACTION_PAYOUT_OPERATOR_RETRY = "PAYOUT_OPERATOR_RETRY";
    public static final String ACTION_PAYOUT_OPERATOR_FAILURE = "PAYOUT_OPERATOR_FAILURE";
    public static final String ACTION_LARGE_REFUND_OR_REVERSAL = "LARGE_REFUND_OR_REVERSAL";
    public static final String ACTION_DISPUTE_RESOLUTION = "DISPUTE_RESOLUTION";

    private final FinancialApprovalStore financialApprovalStore;
    private final AccessPolicy accessPolicy;
    private final RegionService regionService;
    private final ObjectMapper objectMapper;
    private final FinancialApprovalProperties properties;
    private final MerchantFinanceEventService merchantFinanceEventService;

    public FinancialApprovalService(
            FinancialApprovalStore financialApprovalStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            ObjectMapper objectMapper,
            FinancialApprovalProperties properties,
            MerchantFinanceEventService merchantFinanceEventService) {
        this.financialApprovalStore = financialApprovalStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.merchantFinanceEventService = merchantFinanceEventService;
    }

    @Transactional
    public FinancialApprovalRequest createRequest(AccessContext context, CreateApprovalRequestCommand command) {
        requireWrite(context, "financial approval request");
        if (command == null) {
            throw new BadRequestException("approval request body is required.");
        }
        String actionType = requireText(command.actionType(), "actionType");
        String targetType = requireText(command.targetType(), "targetType");
        String reason = requireText(command.reason(), "reason");
        String currencyCode = command.currencyCode() == null || command.currencyCode().isBlank() ? null : command.currencyCode().trim().toUpperCase();
        return financialApprovalStore.createRequest(
                context.workspaceId(),
                actionType,
                targetType,
                command.targetId(),
                command.providerProfileId(),
                currencyCode,
                command.amountMinor(),
                context.actorId(),
                reason,
                json(payload(command)));
    }

    public List<FinancialApprovalRequest> listRequests(AccessContext context) {
        requireRead(context);
        return financialApprovalStore.listRequests(context.workspaceId());
    }

    public ApprovalHistory history(AccessContext context, UUID requestId) {
        requireRead(context);
        FinancialApprovalRequest request = request(context.workspaceId(), requestId);
        return new ApprovalHistory(
                request,
                financialApprovalStore.listDecisions(context.workspaceId(), request.id()),
                financialApprovalStore.executionState(context.workspaceId(), request.id()).orElse(null));
    }

    @Transactional
    public FinancialApprovalRequest approve(AccessContext context, UUID requestId, String note) {
        requireWrite(context, "financial approval decision");
        FinancialApprovalRequest request = request(context.workspaceId(), requestId);
        if (!"REQUESTED".equals(request.status())) {
            throw new BadRequestException("Only requested approvals can be approved.");
        }
        if (request.requestedByActorId().equals(context.actorId())) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Maker and checker must be different actors.");
            throw new BadRequestException("Maker and checker must be different actors.");
        }
        financialApprovalStore.createDecision(context.workspaceId(), request.id(), "APPROVED", context.actorId(), requireText(note, "note"));
        FinancialApprovalRequest approved = financialApprovalStore.updateStatus(context.workspaceId(), request.id(), "APPROVED");
        emitApprovalEvent(approved, MerchantFinanceEventService.EVENT_APPROVAL_GRANTED);
        return approved;
    }

    @Transactional
    public FinancialApprovalRequest reject(AccessContext context, UUID requestId, String note) {
        requireWrite(context, "financial approval decision");
        FinancialApprovalRequest request = request(context.workspaceId(), requestId);
        if (!"REQUESTED".equals(request.status())) {
            throw new BadRequestException("Only requested approvals can be rejected.");
        }
        if (request.requestedByActorId().equals(context.actorId())) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Maker and checker must be different actors.");
            throw new BadRequestException("Maker and checker must be different actors.");
        }
        financialApprovalStore.createDecision(context.workspaceId(), request.id(), "REJECTED", context.actorId(), requireText(note, "note"));
        FinancialApprovalRequest rejected = financialApprovalStore.updateStatus(context.workspaceId(), request.id(), "REJECTED");
        emitApprovalEvent(rejected, MerchantFinanceEventService.EVENT_APPROVAL_REJECTED);
        return rejected;
    }

    public void requireApprovedForExecution(AccessContext context, UUID approvalRequestId, String actionType, String targetType, UUID targetId) {
        if (approvalRequestId == null) {
            throw new BadRequestException(actionType + " requires an approved financial approval request.");
        }
        FinancialApprovalRequest request = request(context.workspaceId(), approvalRequestId);
        if (!actionType.equals(request.actionType()) || !targetType.equals(request.targetType())) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Approval request target does not match the attempted finance action.");
            throw new BadRequestException("Approval request does not match this finance action.");
        }
        if (targetId != null && request.targetId() != null && !targetId.equals(request.targetId())) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Approval request target id does not match the attempted finance action.");
            throw new BadRequestException("Approval request does not match this finance action.");
        }
        if (!"APPROVED".equals(request.status())) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Approval has not been granted.");
            throw new BadRequestException(actionType + " is blocked until approval is granted.");
        }
        List<FinancialApprovalDecision> decisions = financialApprovalStore.listDecisions(context.workspaceId(), request.id());
        boolean checkerDifferentFromMaker = decisions.stream()
                .anyMatch(decision -> "APPROVED".equals(decision.decision()) && !decision.decidedByActorId().equals(request.requestedByActorId()));
        if (!checkerDifferentFromMaker) {
            financialApprovalStore.markBlocked(context.workspaceId(), request.id(), "Maker and checker must be different actors.");
            throw new BadRequestException("Maker and checker must be different actors.");
        }
        financialApprovalStore.markExecuted(context.workspaceId(), request.id(), context.actorId());
    }

    public boolean closeRequiresApproval() {
        return properties.isCloseRequiresApproval();
    }

    public boolean isLargeRefundOrReversal(String refundType, long amountMinor) {
        return "REVERSAL".equals(refundType) || amountMinor >= properties.getLargeRefundThresholdMinor();
    }

    private FinancialApprovalRequest request(UUID workspaceId, UUID requestId) {
        if (requestId == null) {
            throw new BadRequestException("approvalRequestId is required.");
        }
        return financialApprovalStore.findRequest(workspaceId, requestId)
                .orElseThrow(() -> new NotFoundException("Financial approval request was not found."));
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireWrite(AccessContext context, String operation) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
        regionService.requireOwnedForWrite(context, operation);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new BadRequestException("Approval payload could not be serialized.");
        }
    }

    private void emitApprovalEvent(FinancialApprovalRequest request, String eventType) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("approvalRequestId", request.id());
        payload.put("actionType", request.actionType());
        payload.put("targetType", request.targetType());
        payload.put("targetId", request.targetId());
        payload.put("status", request.status());
        merchantFinanceEventService.emit(
                request.workspaceId(),
                request.providerProfileId(),
                request.currencyCode(),
                null,
                eventType,
                "FINANCIAL_APPROVAL_REQUEST",
                request.id(),
                payload);
    }

    private static Map<String, Object> payload(CreateApprovalRequestCommand command) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("actionType", command.actionType());
        payload.put("targetType", command.targetType());
        payload.put("targetId", command.targetId());
        payload.put("providerProfileId", command.providerProfileId());
        payload.put("currencyCode", command.currencyCode());
        payload.put("amountMinor", command.amountMinor());
        return payload;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(fieldName + " is required.");
        }
        return value.trim();
    }

    public record CreateApprovalRequestCommand(
            String actionType,
            String targetType,
            UUID targetId,
            UUID providerProfileId,
            String currencyCode,
            Long amountMinor,
            String reason) {
    }

    public record ApprovalHistory(
            FinancialApprovalRequest request,
            List<FinancialApprovalDecision> decisions,
            FinancialApprovalExecutionState executionState) {
    }
}
