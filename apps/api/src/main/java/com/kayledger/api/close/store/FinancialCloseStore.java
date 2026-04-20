package com.kayledger.api.close.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.close.model.AccountingPeriod;
import com.kayledger.api.close.model.FinalizedProviderStatement;
import com.kayledger.api.close.model.FinancialCloseAuditEvent;
import com.kayledger.api.close.model.FinancialPeriodClose;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Repository
public class FinancialCloseStore {

    private static final RowMapper<AccountingPeriod> PERIOD_MAPPER = (rs, rowNum) -> new AccountingPeriod(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("period_start", LocalDate.class),
            rs.getObject("period_end", LocalDate.class),
            rs.getString("status"),
            rs.getObject("opened_by_actor_id", UUID.class),
            rs.getObject("closed_by_actor_id", UUID.class),
            rs.getObject("reopened_by_actor_id", UUID.class),
            rs.getString("close_reason"),
            rs.getString("reopen_reason"),
            nullableInstant(rs, "closed_at"),
            nullableInstant(rs, "reopened_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FinancialPeriodClose> CLOSE_MAPPER = (rs, rowNum) -> new FinancialPeriodClose(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("accounting_period_id", UUID.class),
            rs.getString("status"),
            rs.getString("close_reason"),
            rs.getString("reopened_reason"),
            rs.getObject("closed_by_actor_id", UUID.class),
            rs.getObject("reopened_by_actor_id", UUID.class),
            instant(rs, "closed_at"),
            nullableInstant(rs, "reopened_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FinalizedProviderStatement> STATEMENT_MAPPER = (rs, rowNum) -> new FinalizedProviderStatement(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("accounting_period_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getObject("period_start", LocalDate.class),
            rs.getObject("period_end", LocalDate.class),
            rs.getString("status"),
            rs.getLong("settled_gross_amount_minor"),
            rs.getLong("fee_amount_minor"),
            rs.getLong("net_earnings_amount_minor"),
            rs.getLong("current_payout_requested_amount_minor"),
            rs.getLong("payout_succeeded_amount_minor"),
            rs.getLong("refund_amount_minor"),
            rs.getLong("active_dispute_exposure_amount_minor"),
            rs.getLong("settled_subscription_net_revenue_amount_minor"),
            rs.getString("snapshot_json"),
            rs.getObject("finalized_by_actor_id", UUID.class),
            instant(rs, "finalized_at"),
            nullableInstant(rs, "reopened_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FinancialCloseAuditEvent> AUDIT_MAPPER = (rs, rowNum) -> new FinancialCloseAuditEvent(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("accounting_period_id", UUID.class),
            rs.getObject("finalized_statement_id", UUID.class),
            rs.getString("event_type"),
            rs.getObject("actor_id", UUID.class),
            rs.getString("reason"),
            instant(rs, "created_at"));

    private final JdbcTemplate jdbcTemplate;

    public FinancialCloseStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public AccountingPeriod createPeriod(UUID workspaceId, LocalDate periodStart, LocalDate periodEnd, UUID actorId) {
        AccountingPeriod period = jdbcTemplate.queryForObject("""
                INSERT INTO financial_accounting_periods (workspace_id, period_start, period_end, opened_by_actor_id)
                VALUES (?, ?, ?, ?)
                RETURNING *
                """, PERIOD_MAPPER, workspaceId, periodStart, periodEnd, actorId);
        recordAudit(workspaceId, period.id(), null, "PERIOD_OPENED", actorId, "Accounting period opened.");
        return period;
    }

    public int countOverlappingPeriods(UUID workspaceId, LocalDate periodStart, LocalDate periodEnd) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM financial_accounting_periods
                WHERE workspace_id = ?
                  AND daterange(period_start, period_end, '[]') && daterange(?::date, ?::date, '[]')
                """, Integer.class, workspaceId, periodStart, periodEnd);
        return value == null ? 0 : value;
    }

    public List<AccountingPeriod> listPeriods(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_accounting_periods
                WHERE workspace_id = ?
                ORDER BY period_start DESC, id
                """, PERIOD_MAPPER, workspaceId);
    }

    public Optional<AccountingPeriod> findPeriod(UUID workspaceId, UUID periodId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_accounting_periods
                WHERE workspace_id = ?
                  AND id = ?
                """, PERIOD_MAPPER, workspaceId, periodId).stream().findFirst();
    }

    public Optional<AccountingPeriod> closedPeriodForPosting(UUID workspaceId, UUID providerProfileId, String currencyCode, Instant occurredAt) {
        return jdbcTemplate.query("""
                SELECT fap.*
                FROM financial_accounting_periods fap
                WHERE fap.workspace_id = ?
                  AND fap.status = 'CLOSED'
                  AND ? >= fap.period_start
                  AND ? < (fap.period_end + 1)
                  AND EXISTS (
                    SELECT 1
                    FROM finalized_provider_statements fps
                    WHERE fps.workspace_id = fap.workspace_id
                      AND fps.accounting_period_id = fap.id
                      AND fps.provider_profile_id = ?
                      AND fps.currency_code = ?
                      AND fps.status = 'FINALIZED'
                  )
                ORDER BY fap.period_start DESC
                LIMIT 1
                """, PERIOD_MAPPER, workspaceId, timestamp(occurredAt), timestamp(occurredAt), providerProfileId, currencyCode).stream().findFirst();
    }

    public AccountingPeriod closePeriod(UUID workspaceId, UUID periodId, UUID actorId, String reason) {
        AccountingPeriod period = jdbcTemplate.queryForObject("""
                UPDATE financial_accounting_periods
                SET status = 'CLOSED',
                    closed_by_actor_id = ?,
                    closed_at = now(),
                    close_reason = ?,
                    reopened_by_actor_id = NULL,
                    reopened_at = NULL,
                    reopen_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'OPEN'
                RETURNING *
                """, PERIOD_MAPPER, actorId, reason, workspaceId, periodId);
        recordAudit(workspaceId, period.id(), null, "PERIOD_CLOSED", actorId, reason);
        return period;
    }

    public FinancialPeriodClose createCloseRecord(UUID workspaceId, UUID periodId, UUID actorId, String reason) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO financial_period_closes (workspace_id, accounting_period_id, close_reason, closed_by_actor_id)
                VALUES (?, ?, ?, ?)
                RETURNING *
                """, CLOSE_MAPPER, workspaceId, periodId, reason, actorId);
    }

    public AccountingPeriod reopenPeriod(UUID workspaceId, UUID periodId, UUID actorId, String reason) {
        AccountingPeriod period = jdbcTemplate.queryForObject("""
                UPDATE financial_accounting_periods
                SET status = 'OPEN',
                    reopened_by_actor_id = ?,
                    reopened_at = now(),
                    reopen_reason = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'CLOSED'
                RETURNING *
                """, PERIOD_MAPPER, actorId, reason, workspaceId, periodId);
        jdbcTemplate.update("""
                UPDATE financial_period_closes
                SET status = 'REOPENED',
                    reopened_by_actor_id = ?,
                    reopened_at = now(),
                    reopened_reason = ?
                WHERE workspace_id = ?
                  AND accounting_period_id = ?
                  AND status = 'CLOSED'
                """, actorId, reason, workspaceId, periodId);
        jdbcTemplate.update("""
                UPDATE finalized_provider_statements
                SET status = 'VOIDED_BY_REOPEN',
                    reopened_at = now()
                WHERE workspace_id = ?
                  AND accounting_period_id = ?
                  AND status = 'FINALIZED'
                """, workspaceId, periodId);
        recordAudit(workspaceId, period.id(), null, "PERIOD_REOPENED", actorId, reason);
        return period;
    }

    public FinalizedProviderStatement createFinalizedStatement(AccountingPeriod period, ProviderFinancialSummary summary, UUID actorId, String snapshotJson) {
        FinalizedProviderStatement statement = jdbcTemplate.queryForObject("""
                INSERT INTO finalized_provider_statements (
                    workspace_id, accounting_period_id, provider_profile_id, currency_code, period_start, period_end,
                    settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                    current_payout_requested_amount_minor, payout_succeeded_amount_minor, refund_amount_minor,
                    active_dispute_exposure_amount_minor, settled_subscription_net_revenue_amount_minor,
                    snapshot_json, finalized_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                RETURNING *, snapshot_json::text
                """, STATEMENT_MAPPER,
                period.workspaceId(), period.id(), summary.providerProfileId(), summary.currencyCode(), period.periodStart(), period.periodEnd(),
                summary.settledGrossAmountMinor(), summary.feeAmountMinor(), summary.netEarningsAmountMinor(),
                summary.currentPayoutRequestedAmountMinor(), summary.payoutSucceededAmountMinor(), summary.refundAmountMinor(),
                summary.activeDisputeExposureAmountMinor(), summary.settledSubscriptionNetRevenueAmountMinor(),
                snapshotJson, actorId);
        recordAudit(period.workspaceId(), period.id(), statement.id(), "STATEMENT_FINALIZED", actorId, "Provider statement finalized for closed period.");
        return statement;
    }

    public List<FinalizedProviderStatement> listFinalizedStatements(UUID workspaceId, UUID periodId) {
        return jdbcTemplate.query("""
                SELECT *, snapshot_json::text
                FROM finalized_provider_statements
                WHERE workspace_id = ?
                  AND accounting_period_id = ?
                ORDER BY provider_profile_id, currency_code, finalized_at, id
                """, STATEMENT_MAPPER, workspaceId, periodId);
    }

    public Optional<FinalizedProviderStatement> findFinalizedStatement(UUID workspaceId, UUID statementId) {
        return jdbcTemplate.query("""
                SELECT *, snapshot_json::text
                FROM finalized_provider_statements
                WHERE workspace_id = ?
                  AND id = ?
                """, STATEMENT_MAPPER, workspaceId, statementId).stream().findFirst();
    }

    public List<FinancialCloseAuditEvent> listAuditEvents(UUID workspaceId, UUID periodId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_close_audit_events
                WHERE workspace_id = ?
                  AND accounting_period_id = ?
                ORDER BY created_at, id
                """, AUDIT_MAPPER, workspaceId, periodId);
    }

    public void recordAudit(UUID workspaceId, UUID periodId, UUID statementId, String eventType, UUID actorId, String reason) {
        jdbcTemplate.update("""
                INSERT INTO financial_close_audit_events (
                    workspace_id, accounting_period_id, finalized_statement_id, event_type, actor_id, reason
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """, workspaceId, periodId, statementId, eventType, actorId, reason);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }
}
