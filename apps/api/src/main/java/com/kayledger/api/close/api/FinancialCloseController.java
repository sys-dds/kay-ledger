package com.kayledger.api.close.api;

import java.time.LocalDate;
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
import com.kayledger.api.close.application.FinancialCloseService;
import com.kayledger.api.close.model.AccountingPeriod;
import com.kayledger.api.close.model.FinalizedProviderStatement;
import com.kayledger.api.close.model.FinancialCloseAuditEvent;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/financial-close")
public class FinancialCloseController {

    private final AccessContextResolver accessContextResolver;
    private final FinancialCloseService financialCloseService;
    private final IdempotencyService idempotencyService;

    public FinancialCloseController(
            AccessContextResolver accessContextResolver,
            FinancialCloseService financialCloseService,
            IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.financialCloseService = financialCloseService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/periods")
    ResponseEntity<Object> openPeriod(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody OpenPeriodRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("open period request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-close/periods",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financialCloseService.openPeriod(context, request.periodStart(), request.periodEnd()));
    }

    @GetMapping("/periods")
    List<AccountingPeriod> listPeriods(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return financialCloseService.listPeriods(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/periods/{periodId}")
    AccountingPeriod periodDetails(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID periodId) {
        return financialCloseService.periodDetails(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), periodId);
    }

    @PostMapping("/periods/{periodId}/close")
    ResponseEntity<Object> closePeriod(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID periodId,
            @RequestBody ClosePeriodRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("close period request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-close/periods/{periodId}/close",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, periodId, request),
                () -> financialCloseService.closePeriod(context, periodId, request.closeReason()));
    }

    @PostMapping("/periods/{periodId}/reopen")
    ResponseEntity<Object> reopenPeriod(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID periodId,
            @RequestBody ReopenPeriodRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        if (request == null) {
            throw new BadRequestException("reopen period request is required.");
        }
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/financial-close/periods/{periodId}/reopen",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, periodId, request),
                () -> financialCloseService.reopenPeriod(context, periodId, request.reopenReason()));
    }

    @GetMapping("/periods/{periodId}/statements")
    List<FinalizedProviderStatement> finalizedStatements(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID periodId) {
        return financialCloseService.finalizedStatements(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), periodId);
    }

    @GetMapping("/statements/{statementId}")
    FinalizedProviderStatement statementDetails(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID statementId) {
        return financialCloseService.finalizedStatementDetails(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), statementId);
    }

    @GetMapping("/periods/{periodId}/audit-events")
    List<FinancialCloseAuditEvent> auditEvents(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID periodId) {
        return financialCloseService.auditEvents(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), periodId);
    }

    public record OpenPeriodRequest(LocalDate periodStart, LocalDate periodEnd) {
    }

    public record ClosePeriodRequest(String closeReason) {
    }

    public record ReopenPeriodRequest(String reopenReason) {
    }
}
