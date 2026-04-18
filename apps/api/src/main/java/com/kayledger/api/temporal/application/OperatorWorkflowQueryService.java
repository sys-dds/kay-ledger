package com.kayledger.api.temporal.application;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.shared.api.NotFoundException;
import com.kayledger.api.temporal.model.OperatorWorkflowStatus;

@Service
public class OperatorWorkflowQueryService {

    private final OperatorWorkflowService operatorWorkflowService;
    private final AccessPolicy accessPolicy;

    public OperatorWorkflowQueryService(OperatorWorkflowService operatorWorkflowService, AccessPolicy accessPolicy) {
        this.operatorWorkflowService = operatorWorkflowService;
        this.accessPolicy = accessPolicy;
    }

    public OperatorWorkflowStatus findByReference(AccessContext context, String workflowType, String referenceType, UUID referenceId) {
        OperatorWorkflowService.WorkflowTypePolicy policy = requireRead(context, workflowType);
        if (!policy.businessReferenceType().equals(referenceType)) {
            throw new NotFoundException("Operator workflow was not found.");
        }
        OperatorWorkflowRecord record = operatorWorkflowService.findByReference(context.workspaceId(), referenceType, referenceId)
                .filter(candidate -> workflowType.equals(candidate.workflowType()))
                .orElseThrow(() -> new NotFoundException("Operator workflow was not found."));
        return toStatus(record);
    }

    public List<OperatorWorkflowStatus> list(AccessContext context, String workflowType) {
        requireRead(context, workflowType);
        return operatorWorkflowService.list(context.workspaceId(), workflowType).stream()
                .map(this::toStatus)
                .toList();
    }

    private OperatorWorkflowStatus toStatus(OperatorWorkflowRecord record) {
        return new OperatorWorkflowStatus(
                record.id(),
                record.workspaceId(),
                record.workflowType(),
                record.businessReferenceType(),
                record.businessReferenceId(),
                record.workflowId(),
                record.runId(),
                record.triggerMode(),
                record.status(),
                record.progressCurrent(),
                record.progressTotal(),
                record.progressMessage(),
                record.progressUpdateCount(),
                record.requestedAt(),
                record.startedAt(),
                record.completedAt(),
                record.failureReason());
    }

    private OperatorWorkflowService.WorkflowTypePolicy requireRead(AccessContext context, String workflowType) {
        OperatorWorkflowService.WorkflowTypePolicy policy = operatorWorkflowService.policy(workflowType);
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, policy.readScope());
        return policy;
    }
}
