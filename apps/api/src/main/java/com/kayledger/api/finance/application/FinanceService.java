package com.kayledger.api.finance.application;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.finance.model.AccountBalance;
import com.kayledger.api.finance.model.FeeBreakdown;
import com.kayledger.api.finance.model.FeeRule;
import com.kayledger.api.finance.model.FinancialAccount;
import com.kayledger.api.finance.model.JournalEntry;
import com.kayledger.api.finance.model.JournalEntryDetails;
import com.kayledger.api.finance.model.JournalPosting;
import com.kayledger.api.finance.model.Money;
import com.kayledger.api.finance.store.FinanceStore;
import com.kayledger.api.booking.store.BookingStore;
import com.kayledger.api.shared.api.BadRequestException;

@Service
public class FinanceService {

    private static final Set<String> ACCOUNT_TYPES = Set.of("ASSET", "LIABILITY", "REVENUE", "EXPENSE", "EQUITY");
    private static final Set<String> ACCOUNT_PURPOSES = Set.of(
            "PLATFORM_RECEIVABLE",
            "CASH_PLACEHOLDER",
            "CUSTOMER_PAYABLE",
            "SELLER_PAYABLE",
            "FEE_REVENUE",
            "REFUND_RESERVE",
            "REFUND_LIABILITY",
            "DISPUTE_RESERVE",
            "PAYOUT_CLEARING",
            "FROZEN_PAYABLE",
            "CLEARING",
            "SUSPENSE",
            "AUTHORIZED_FUNDS",
            "CAPTURED_FUNDS",
            "PLATFORM_CLEARING");
    private static final Set<String> FEE_RULE_TYPES = Set.of("FLAT", "BASIS_POINTS", "COMBINED");
    private static final Set<String> REFERENCE_TYPES = Set.of("BOOKING", "OFFERING", "MANUAL", "EXTERNAL", "PAYMENT", "PAYOUT", "REFUND", "DISPUTE", "SUBSCRIPTION", "SUBSCRIPTION_CYCLE");
    private static final Set<String> ENTRY_SIDES = Set.of("DEBIT", "CREDIT");

    private final FinanceStore financeStore;
    private final AccessPolicy accessPolicy;
    private final BookingStore bookingStore;

    public FinanceService(FinanceStore financeStore, AccessPolicy accessPolicy, BookingStore bookingStore) {
        this.financeStore = financeStore;
        this.accessPolicy = accessPolicy;
        this.bookingStore = bookingStore;
    }

    public FinancialAccount createAccount(AccessContext context, CreateAccountCommand command) {
        requireFinanceWrite(context);
        return financeStore.createAccount(
                context.workspaceId(),
                requireText(command.accountCode(), "accountCode"),
                requireText(command.accountName(), "accountName"),
                requireOneOf(command.accountType(), ACCOUNT_TYPES, "accountType"),
                requireOneOf(command.accountPurpose(), ACCOUNT_PURPOSES, "accountPurpose"),
                Money.requireCurrency(command.currencyCode()));
    }

    public List<FinancialAccount> listAccounts(AccessContext context) {
        requireFinanceRead(context);
        return financeStore.listAccounts(context.workspaceId());
    }

    public FeeRule createFeeRule(AccessContext context, CreateFeeRuleCommand command) {
        requireFinanceWrite(context);
        String ruleType = requireOneOf(command.ruleType(), FEE_RULE_TYPES, "ruleType");
        Long flatAmountMinor = command.flatAmountMinor();
        Integer basisPoints = command.basisPoints();
        if ("FLAT".equals(ruleType) && (flatAmountMinor == null || basisPoints != null)) {
            throw new BadRequestException("FLAT fee rules require flatAmountMinor only.");
        }
        if ("BASIS_POINTS".equals(ruleType) && (flatAmountMinor != null || basisPoints == null)) {
            throw new BadRequestException("BASIS_POINTS fee rules require basisPoints only.");
        }
        if ("COMBINED".equals(ruleType) && (flatAmountMinor == null || basisPoints == null)) {
            throw new BadRequestException("COMBINED fee rules require flatAmountMinor and basisPoints.");
        }
        if (flatAmountMinor != null && flatAmountMinor < 0) {
            throw new BadRequestException("flatAmountMinor must be zero or greater.");
        }
        if (basisPoints != null && (basisPoints < 0 || basisPoints > 10000)) {
            throw new BadRequestException("basisPoints must be between 0 and 10000.");
        }
        return financeStore.createFeeRule(
                context.workspaceId(),
                command.offeringId(),
                ruleType,
                flatAmountMinor,
                basisPoints,
                Money.requireCurrency(command.currencyCode()));
    }

    public List<FeeRule> listFeeRules(AccessContext context) {
        requireFinanceRead(context);
        return financeStore.listFeeRules(context.workspaceId());
    }

    public FeeBreakdown calculateFees(UUID workspaceId, UUID offeringId, Money grossAmount) {
        List<FeeRule> rules = financeStore.listActiveFeeRulesForOffering(workspaceId, offeringId);
        long feeAmountMinor = 0;
        for (FeeRule rule : rules) {
            if (!grossAmount.currencyCode().equals(rule.currencyCode())) {
                throw new BadRequestException("Fee rule currency does not match the booking currency.");
            }
            if ("FLAT".equals(rule.ruleType()) && (rule.flatAmountMinor() == null || rule.basisPoints() != null)) {
                throw new BadRequestException("FLAT fee rule configuration is invalid.");
            }
            if ("BASIS_POINTS".equals(rule.ruleType()) && (rule.flatAmountMinor() != null || rule.basisPoints() == null)) {
                throw new BadRequestException("BASIS_POINTS fee rule configuration is invalid.");
            }
            if ("COMBINED".equals(rule.ruleType()) && (rule.flatAmountMinor() == null || rule.basisPoints() == null)) {
                throw new BadRequestException("COMBINED fee rule configuration is invalid.");
            }
            if (rule.flatAmountMinor() != null) {
                feeAmountMinor += rule.flatAmountMinor();
            }
            if (rule.basisPoints() != null) {
                feeAmountMinor += (grossAmount.amountMinor() * rule.basisPoints()) / 10_000;
            }
        }
        if (feeAmountMinor > grossAmount.amountMinor()) {
            throw new BadRequestException("Fee amount cannot exceed gross amount.");
        }
        Money fee = new Money(grossAmount.currencyCode(), feeAmountMinor);
        return new FeeBreakdown(grossAmount, fee, grossAmount.minus(fee));
    }

    @Transactional
    public JournalEntryDetails createJournalEntry(AccessContext context, CreateJournalEntryCommand command) {
        requireFinanceWrite(context);
        return createJournalEntryForReference(context.workspaceId(), command);
    }

    @Transactional
    public JournalEntryDetails createJournalEntryForReference(UUID workspaceId, CreateJournalEntryCommand command) {
        validateBalanced(command.postings());
        validatePostingAccounts(workspaceId, command.postings());
        JournalEntry entry = financeStore.createJournalEntry(
                workspaceId,
                requireOneOf(command.referenceType(), REFERENCE_TYPES, "referenceType"),
                command.referenceId(),
                command.offeringId(),
                command.bookingId(),
                blankToNull(command.externalReference()),
                requireText(command.description(), "description"));
        List<JournalPosting> postings = command.postings().stream()
                .map(posting -> financeStore.createPosting(
                        entry.id(),
                        posting.accountId(),
                        requireOneOf(posting.entrySide(), ENTRY_SIDES, "entrySide"),
                        requirePositive(posting.amountMinor(), "amountMinor"),
                        Money.requireCurrency(posting.currencyCode())))
                .toList();
        if ("BOOKING".equals(command.referenceType()) && command.bookingId() != null) {
            bookingStore.attachFinancialReference(workspaceId, command.bookingId(), entry.id());
        }
        return new JournalEntryDetails(entry, postings);
    }

    public FinancialAccount accountByPurpose(UUID workspaceId, String accountPurpose, String currencyCode) {
        return financeStore.findAccountByPurpose(
                        workspaceId,
                        requireOneOf(accountPurpose, ACCOUNT_PURPOSES, "accountPurpose"),
                        Money.requireCurrency(currencyCode))
                .orElseThrow(() -> new BadRequestException("Required financial account purpose is not configured for this workspace."));
    }

    public List<JournalEntryDetails> listJournalEntries(AccessContext context) {
        requireFinanceRead(context);
        return financeStore.listJournalEntries(context.workspaceId()).stream()
                .map(this::details)
                .toList();
    }

    public List<JournalEntryDetails> listJournalEntriesByReference(AccessContext context, String referenceType, UUID referenceId) {
        requireFinanceRead(context);
        return financeStore.listJournalEntriesByReference(
                        context.workspaceId(),
                        requireOneOf(referenceType, REFERENCE_TYPES, "referenceType"),
                        requireId(referenceId, "referenceId")).stream()
                .map(this::details)
                .toList();
    }

    public AccountBalance balance(AccessContext context, UUID accountId) {
        requireFinanceRead(context);
        FinancialAccount account = financeStore.findAccount(context.workspaceId(), requireId(accountId, "accountId"))
                .orElseThrow(() -> new BadRequestException("accountId is not valid for this workspace."));
        return financeStore.balance(account);
    }

    private JournalEntryDetails details(JournalEntry entry) {
        return new JournalEntryDetails(entry, financeStore.listPostings(entry.id()));
    }

    private void validateBalanced(List<PostingCommand> postings) {
        if (postings == null || postings.size() < 2) {
            throw new BadRequestException("A journal entry requires at least two postings.");
        }
        String currency = Money.requireCurrency(postings.get(0).currencyCode());
        long debits = 0;
        long credits = 0;
        for (PostingCommand posting : postings) {
            Money.requireCurrency(posting.currencyCode());
            if (!currency.equals(posting.currencyCode())) {
                throw new BadRequestException("Journal postings must use one currency.");
            }
            long amount = requirePositive(posting.amountMinor(), "amountMinor");
            String side = requireOneOf(posting.entrySide(), ENTRY_SIDES, "entrySide");
            if ("DEBIT".equals(side)) {
                debits += amount;
            } else {
                credits += amount;
            }
        }
        if (debits != credits) {
            throw new BadRequestException("Journal entry postings must balance.");
        }
    }

    private void validatePostingAccounts(UUID workspaceId, List<PostingCommand> postings) {
        for (PostingCommand posting : postings) {
            FinancialAccount account = financeStore.findAccount(workspaceId, requireId(posting.accountId(), "accountId"))
                    .orElseThrow(() -> new BadRequestException("Journal posting account is not valid for this workspace."));
            if (!account.currencyCode().equals(Money.requireCurrency(posting.currencyCode()))) {
                throw new BadRequestException("Journal posting currency must match the financial account currency.");
            }
        }
    }

    private void requireFinanceRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireFinanceWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireOneOf(String value, Set<String> allowed, String field) {
        String required = requireText(value, field);
        if (!allowed.contains(required)) {
            throw new BadRequestException(field + " is invalid.");
        }
        return required;
    }

    private static long requirePositive(Long value, String field) {
        if (value == null || value <= 0) {
            throw new BadRequestException(field + " must be greater than zero.");
        }
        return value;
    }

    private static UUID requireId(UUID value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public record CreateAccountCommand(
            String accountCode,
            String accountName,
            String accountType,
            String accountPurpose,
            String currencyCode) {
    }

    public record CreateFeeRuleCommand(
            UUID offeringId,
            String ruleType,
            Long flatAmountMinor,
            Integer basisPoints,
            String currencyCode) {
    }

    public record CreateJournalEntryCommand(
            String referenceType,
            UUID referenceId,
            UUID offeringId,
            UUID bookingId,
            String externalReference,
            String description,
            List<PostingCommand> postings) {
    }

    public record PostingCommand(
            UUID accountId,
            String entrySide,
            Long amountMinor,
            String currencyCode) {
    }
}
