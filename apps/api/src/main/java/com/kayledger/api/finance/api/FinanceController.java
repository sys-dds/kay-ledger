package com.kayledger.api.finance.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.finance.application.FinanceService;
import com.kayledger.api.finance.application.FinanceService.CreateAccountCommand;
import com.kayledger.api.finance.application.FinanceService.CreateFeeRuleCommand;
import com.kayledger.api.finance.application.FinanceService.CreateJournalEntryCommand;
import com.kayledger.api.finance.model.AccountBalance;
import com.kayledger.api.finance.model.FeeRule;
import com.kayledger.api.finance.model.FinancialAccount;
import com.kayledger.api.finance.model.JournalEntryDetails;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/finance")
public class FinanceController {

    private final FinanceService financeService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;

    public FinanceController(
            FinanceService financeService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService) {
        this.financeService = financeService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/accounts")
    ResponseEntity<Object> createAccount(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateAccountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/finance/accounts",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financeService.createAccount(context, request));
    }

    @GetMapping("/accounts")
    List<FinancialAccount> listAccounts(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return financeService.listAccounts(context);
    }

    @PostMapping("/fee-rules")
    ResponseEntity<Object> createFeeRule(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateFeeRuleCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/finance/fee-rules",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financeService.createFeeRule(context, request));
    }

    @GetMapping("/fee-rules")
    List<FeeRule> listFeeRules(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return financeService.listFeeRules(context);
    }

    @PostMapping("/journal-entries")
    ResponseEntity<Object> createJournalEntry(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateJournalEntryCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/finance/journal-entries",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> financeService.createJournalEntry(context, request));
    }

    @GetMapping("/journal-entries")
    List<JournalEntryDetails> listJournalEntries(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return financeService.listJournalEntries(context);
    }

    @GetMapping("/journal-entries/by-reference")
    List<JournalEntryDetails> journalEntriesByReference(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestParam String referenceType,
            @RequestParam UUID referenceId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return financeService.listJournalEntriesByReference(context, referenceType, referenceId);
    }

    @GetMapping("/accounts/{accountId}/balance")
    AccountBalance balance(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID accountId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return financeService.balance(context, accountId);
    }
}
