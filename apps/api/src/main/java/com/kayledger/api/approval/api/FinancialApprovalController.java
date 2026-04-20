package com.kayledger.api.approval.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.approval.application.FinancialApprovalService;
import com.kayledger.api.approval.application.FinancialApprovalService.ApprovalHistory;
import com.kayledger.api.approval.application.FinancialApprovalService.CreateApprovalRequestCommand;
import com.kayledger.api.approval.model.FinancialApprovalRequest;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/financial-approvals")
public class FinancialApprovalController {

    private final AccessContextResolver accessContextResolver;
    private final FinancialApprovalService financialApprovalService;
    private final IdempotencyService idempotencyService;

    public FinancialApprovalController(AccessContextResolver accessContextResolver, FinancialApprovalService financialApprovalService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.financialApprovalService = financialApprovalService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    ResponseEntity<Object> createRequest(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateApprovalRequestCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("approval request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-approvals",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financialApprovalService.createRequest(context, request));
    }

    @GetMapping
    List<FinancialApprovalRequest> listRequests(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return financialApprovalService.listRequests(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/{approvalRequestId}/history")
    ApprovalHistory history(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID approvalRequestId) {
        return financialApprovalService.history(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), approvalRequestId);
    }

    @PostMapping("/{approvalRequestId}/approve")
    ResponseEntity<Object> approve(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID approvalRequestId,
            @RequestBody ApprovalDecisionRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-approvals/{approvalRequestId}/approve",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, approvalRequestId, request),
                () -> financialApprovalService.approve(context, approvalRequestId, request == null ? null : request.note()));
    }

    @PostMapping("/{approvalRequestId}/reject")
    ResponseEntity<Object> reject(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID approvalRequestId,
            @RequestBody ApprovalDecisionRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-approvals/{approvalRequestId}/reject",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, approvalRequestId, request),
                () -> financialApprovalService.reject(context, approvalRequestId, request == null ? null : request.note()));
    }

    public record ApprovalDecisionRequest(String note) {
    }
}
