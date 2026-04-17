package com.kayledger.api.finance.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.finance.model.AccountBalance;
import com.kayledger.api.finance.model.FeeRule;
import com.kayledger.api.finance.model.FinancialAccount;
import com.kayledger.api.finance.model.JournalEntry;
import com.kayledger.api.finance.model.JournalPosting;

@Repository
public class FinanceStore {

    private static final RowMapper<FinancialAccount> ACCOUNT_MAPPER = (rs, rowNum) -> new FinancialAccount(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("account_code"),
            rs.getString("account_name"),
            rs.getString("account_type"),
            rs.getString("account_purpose"),
            rs.getString("currency_code"),
            rs.getString("status"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FeeRule> FEE_RULE_MAPPER = (rs, rowNum) -> new FeeRule(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("offering_id", UUID.class),
            rs.getString("rule_type"),
            (Long) rs.getObject("flat_amount_minor"),
            (Integer) rs.getObject("basis_points"),
            rs.getString("currency_code"),
            rs.getString("status"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<JournalEntry> JOURNAL_ENTRY_MAPPER = (rs, rowNum) -> new JournalEntry(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("reference_type"),
            rs.getObject("reference_id", UUID.class),
            rs.getObject("offering_id", UUID.class),
            rs.getObject("booking_id", UUID.class),
            rs.getString("external_reference"),
            rs.getString("description"),
            rs.getString("status"),
            instant(rs, "created_at"));

    private static final RowMapper<JournalPosting> POSTING_MAPPER = (rs, rowNum) -> new JournalPosting(
            rs.getObject("id", UUID.class),
            rs.getObject("journal_entry_id", UUID.class),
            rs.getObject("account_id", UUID.class),
            rs.getString("entry_side"),
            rs.getLong("amount_minor"),
            rs.getString("currency_code"),
            instant(rs, "created_at"));

    private static final RowMapper<AccountBalance> BALANCE_MAPPER = (rs, rowNum) -> new AccountBalance(
            rs.getObject("account_id", UUID.class),
            rs.getString("currency_code"),
            rs.getLong("debit_amount_minor"),
            rs.getLong("credit_amount_minor"),
            rs.getLong("signed_balance_minor"));

    private final JdbcTemplate jdbcTemplate;

    public FinanceStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public FinancialAccount createAccount(
            UUID workspaceId,
            String accountCode,
            String accountName,
            String accountType,
            String accountPurpose,
            String currencyCode) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO financial_accounts (
                    workspace_id,
                    account_code,
                    account_name,
                    account_type,
                    account_purpose,
                    currency_code
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING *
                """, ACCOUNT_MAPPER, workspaceId, accountCode, accountName, accountType, accountPurpose, currencyCode);
    }

    public List<FinancialAccount> listAccounts(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_accounts
                WHERE workspace_id = ?
                  AND status = 'ACTIVE'
                ORDER BY account_code
                """, ACCOUNT_MAPPER, workspaceId);
    }

    public Optional<FinancialAccount> findAccount(UUID workspaceId, UUID accountId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_accounts
                WHERE workspace_id = ?
                  AND id = ?
                """, ACCOUNT_MAPPER, workspaceId, accountId).stream().findFirst();
    }

    public Optional<FinancialAccount> findAccountByPurpose(UUID workspaceId, String accountPurpose, String currencyCode) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_accounts
                WHERE workspace_id = ?
                  AND account_purpose = ?
                  AND currency_code = ?
                  AND status = 'ACTIVE'
                ORDER BY account_code
                LIMIT 1
                """, ACCOUNT_MAPPER, workspaceId, accountPurpose, currencyCode).stream().findFirst();
    }

    public FeeRule createFeeRule(UUID workspaceId, UUID offeringId, String ruleType, Long flatAmountMinor, Integer basisPoints, String currencyCode) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO fee_rules (
                    workspace_id,
                    offering_id,
                    rule_type,
                    flat_amount_minor,
                    basis_points,
                    currency_code
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING *
                """, FEE_RULE_MAPPER, workspaceId, offeringId, ruleType, flatAmountMinor, basisPoints, currencyCode);
    }

    public List<FeeRule> listFeeRules(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM fee_rules
                WHERE workspace_id = ?
                  AND status = 'ACTIVE'
                ORDER BY created_at, id
                """, FEE_RULE_MAPPER, workspaceId);
    }

    public List<FeeRule> listActiveFeeRulesForOffering(UUID workspaceId, UUID offeringId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM fee_rules
                WHERE workspace_id = ?
                  AND status = 'ACTIVE'
                  AND (offering_id = ? OR offering_id IS NULL)
                ORDER BY offering_id NULLS LAST, created_at, id
                """, FEE_RULE_MAPPER, workspaceId, offeringId);
    }

    public JournalEntry createJournalEntry(
            UUID workspaceId,
            String referenceType,
            UUID referenceId,
            UUID offeringId,
            UUID bookingId,
            String externalReference,
            String description) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO journal_entries (
                    workspace_id,
                    reference_type,
                    reference_id,
                    offering_id,
                    booking_id,
                    external_reference,
                    description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, JOURNAL_ENTRY_MAPPER, workspaceId, referenceType, referenceId, offeringId, bookingId, externalReference, description);
    }

    public JournalPosting createPosting(UUID journalEntryId, UUID accountId, String entrySide, long amountMinor, String currencyCode) {
        List<JournalPosting> postings = jdbcTemplate.query("""
                INSERT INTO journal_postings (
                    journal_entry_id,
                    account_id,
                    entry_side,
                    amount_minor,
                    currency_code
                )
                SELECT ?, ?, ?, ?, ?
                FROM journal_entries entry
                JOIN financial_accounts account ON account.id = ?
                WHERE entry.id = ?
                  AND entry.workspace_id = account.workspace_id
                  AND account.status = 'ACTIVE'
                  AND account.currency_code = ?
                RETURNING *
                """, POSTING_MAPPER, journalEntryId, accountId, entrySide, amountMinor, currencyCode, accountId, journalEntryId, currencyCode);
        if (postings.isEmpty()) {
            throw new BadRequestException("Journal posting account is not valid for this journal workspace.");
        }
        return postings.get(0);
    }

    public List<JournalEntry> listJournalEntries(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM journal_entries
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, JOURNAL_ENTRY_MAPPER, workspaceId);
    }

    public List<JournalEntry> listJournalEntriesByReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM journal_entries
                WHERE workspace_id = ?
                  AND reference_type = ?
                  AND reference_id = ?
                ORDER BY created_at, id
                """, JOURNAL_ENTRY_MAPPER, workspaceId, referenceType, referenceId);
    }

    public List<JournalPosting> listPostings(UUID journalEntryId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM journal_postings
                WHERE journal_entry_id = ?
                ORDER BY created_at, id
                """, POSTING_MAPPER, journalEntryId);
    }

    public AccountBalance balance(FinancialAccount account) {
        List<AccountBalance> balances = jdbcTemplate.query("""
                SELECT
                    account.id AS account_id,
                    account.currency_code AS currency_code,
                    COALESCE(SUM(CASE WHEN entry_side = 'DEBIT' THEN amount_minor ELSE 0 END), 0) AS debit_amount_minor,
                    COALESCE(SUM(CASE WHEN entry_side = 'CREDIT' THEN amount_minor ELSE 0 END), 0) AS credit_amount_minor,
                    COALESCE(SUM(CASE WHEN entry_side = 'DEBIT' THEN amount_minor ELSE -amount_minor END), 0) AS signed_balance_minor
                FROM financial_accounts account
                LEFT JOIN journal_postings posting ON posting.account_id = account.id
                WHERE account.id = ?
                GROUP BY account.id, account.currency_code
                """, BALANCE_MAPPER, account.id());
        if (balances.isEmpty()) {
            return new AccountBalance(account.id(), account.currencyCode(), 0, 0, 0);
        }
        return balances.get(0);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
