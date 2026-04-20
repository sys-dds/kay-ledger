package com.kayledger.api.reconciliation.store;

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

import com.kayledger.api.reconciliation.model.ProviderTruthImport;
import com.kayledger.api.reconciliation.model.ProviderTruthSnapshot;
import com.kayledger.api.reconciliation.model.ReconciliationItem;
import com.kayledger.api.reconciliation.model.ReconciliationRun;

@Repository
public class ReconciliationStore {

    private static final RowMapper<ProviderTruthImport> IMPORT_MAPPER = (rs, rowNum) -> new ProviderTruthImport(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getObject("statement_period_start", LocalDate.class),
            rs.getObject("statement_period_end", LocalDate.class),
            rs.getString("source_reference"),
            rs.getString("source_type"),
            rs.getString("status"),
            rs.getObject("imported_by_actor_id", UUID.class),
            instant(rs, "imported_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ProviderTruthSnapshot> SNAPSHOT_MAPPER = (rs, rowNum) -> new ProviderTruthSnapshot(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("truth_import_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getObject("statement_period_start", LocalDate.class),
            rs.getObject("statement_period_end", LocalDate.class),
            rs.getString("source_reference"),
            rs.getLong("settled_gross_amount_minor"),
            rs.getLong("fee_amount_minor"),
            rs.getLong("net_earnings_amount_minor"),
            rs.getLong("payout_succeeded_amount_minor"),
            rs.getLong("refund_amount_minor"),
            rs.getLong("active_dispute_exposure_amount_minor"),
            rs.getLong("settled_subscription_net_revenue_amount_minor"),
            rs.getString("provider_payload_json"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ReconciliationRun> RUN_MAPPER = (rs, rowNum) -> new ReconciliationRun(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("truth_import_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getObject("statement_period_start", LocalDate.class),
            rs.getObject("statement_period_end", LocalDate.class),
            rs.getString("source_reference"),
            rs.getString("status"),
            rs.getObject("started_by_actor_id", UUID.class),
            instant(rs, "started_at"),
            nullableInstant(rs, "completed_at"),
            rs.getInt("unresolved_item_count"),
            rs.getInt("resolved_item_count"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ReconciliationItem> ITEM_MAPPER = (rs, rowNum) -> new ReconciliationItem(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("reconciliation_run_id", UUID.class),
            rs.getObject("truth_import_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getString("source_reference"),
            rs.getString("mismatch_type"),
            rs.getObject("internal_settled_gross_amount_minor", Long.class),
            rs.getObject("provider_settled_gross_amount_minor", Long.class),
            rs.getObject("internal_fee_amount_minor", Long.class),
            rs.getObject("provider_fee_amount_minor", Long.class),
            rs.getObject("internal_net_earnings_amount_minor", Long.class),
            rs.getObject("provider_net_earnings_amount_minor", Long.class),
            rs.getObject("internal_payout_succeeded_amount_minor", Long.class),
            rs.getObject("provider_payout_succeeded_amount_minor", Long.class),
            rs.getObject("internal_refund_amount_minor", Long.class),
            rs.getObject("provider_refund_amount_minor", Long.class),
            rs.getObject("internal_active_dispute_exposure_amount_minor", Long.class),
            rs.getObject("provider_active_dispute_exposure_amount_minor", Long.class),
            rs.getObject("internal_settled_subscription_net_revenue_amount_minor", Long.class),
            rs.getObject("provider_settled_subscription_net_revenue_amount_minor", Long.class),
            rs.getString("detail_json"),
            rs.getString("status"),
            rs.getString("resolution_outcome"),
            rs.getString("resolution_note"),
            rs.getObject("resolved_by_actor_id", UUID.class),
            nullableInstant(rs, "resolved_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public ReconciliationStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ProviderTruthImport upsertProviderTruthImport(UUID workspaceId, UUID providerProfileId, String currencyCode, LocalDate periodStart, LocalDate periodEnd, String sourceReference, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_reconciliation_truth_imports (
                    workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end,
                    source_reference, imported_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, provider_profile_id, currency_code, statement_period_start, statement_period_end, source_reference)
                DO UPDATE SET imported_by_actor_id = EXCLUDED.imported_by_actor_id,
                              status = 'RECORDED'
                RETURNING *
                """, IMPORT_MAPPER, workspaceId, providerProfileId, currencyCode, periodStart, periodEnd, sourceReference, actorId);
    }

    public ProviderTruthSnapshot upsertProviderTruthSnapshot(ProviderTruthImport truthImport, ProviderTruthSnapshotDraft draft) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_reconciliation_truth_snapshots (
                    workspace_id, truth_import_id, provider_profile_id, currency_code,
                    statement_period_start, statement_period_end, source_reference,
                    settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                    payout_succeeded_amount_minor, refund_amount_minor, active_dispute_exposure_amount_minor,
                    settled_subscription_net_revenue_amount_minor, provider_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (workspace_id, truth_import_id, provider_profile_id, currency_code)
                DO UPDATE SET settled_gross_amount_minor = EXCLUDED.settled_gross_amount_minor,
                              fee_amount_minor = EXCLUDED.fee_amount_minor,
                              net_earnings_amount_minor = EXCLUDED.net_earnings_amount_minor,
                              payout_succeeded_amount_minor = EXCLUDED.payout_succeeded_amount_minor,
                              refund_amount_minor = EXCLUDED.refund_amount_minor,
                              active_dispute_exposure_amount_minor = EXCLUDED.active_dispute_exposure_amount_minor,
                              settled_subscription_net_revenue_amount_minor = EXCLUDED.settled_subscription_net_revenue_amount_minor,
                              provider_payload_json = EXCLUDED.provider_payload_json
                RETURNING id, workspace_id, truth_import_id, provider_profile_id, currency_code,
                          statement_period_start, statement_period_end, source_reference,
                          settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                          payout_succeeded_amount_minor, refund_amount_minor, active_dispute_exposure_amount_minor,
                          settled_subscription_net_revenue_amount_minor, provider_payload_json::text, created_at, updated_at
                """, SNAPSHOT_MAPPER,
                truthImport.workspaceId(), truthImport.id(), truthImport.providerProfileId(), truthImport.currencyCode(),
                truthImport.statementPeriodStart(), truthImport.statementPeriodEnd(), truthImport.sourceReference(),
                draft.settledGrossAmountMinor(), draft.feeAmountMinor(), draft.netEarningsAmountMinor(),
                draft.payoutSucceededAmountMinor(), draft.refundAmountMinor(), draft.activeDisputeExposureAmountMinor(),
                draft.settledSubscriptionNetRevenueAmountMinor(), draft.providerPayloadJson());
    }

    public Optional<ProviderTruthImport> findProviderTruthImport(UUID workspaceId, UUID truthImportId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_reconciliation_truth_imports
                WHERE workspace_id = ?
                  AND id = ?
                """, IMPORT_MAPPER, workspaceId, truthImportId).stream().findFirst();
    }

    public Optional<ProviderTruthSnapshot> findProviderTruthSnapshot(UUID workspaceId, UUID truthImportId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, truth_import_id, provider_profile_id, currency_code,
                       statement_period_start, statement_period_end, source_reference,
                       settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                       payout_succeeded_amount_minor, refund_amount_minor, active_dispute_exposure_amount_minor,
                       settled_subscription_net_revenue_amount_minor, provider_payload_json::text, created_at, updated_at
                FROM provider_reconciliation_truth_snapshots
                WHERE workspace_id = ?
                  AND truth_import_id = ?
                """, SNAPSHOT_MAPPER, workspaceId, truthImportId).stream().findFirst();
    }

    public ReconciliationRun createRun(ProviderTruthImport truthImport, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_reconciliation_runs (
                    workspace_id, truth_import_id, provider_profile_id, currency_code,
                    statement_period_start, statement_period_end, source_reference, started_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, RUN_MAPPER, truthImport.workspaceId(), truthImport.id(), truthImport.providerProfileId(), truthImport.currencyCode(),
                truthImport.statementPeriodStart(), truthImport.statementPeriodEnd(), truthImport.sourceReference(), actorId);
    }

    public ReconciliationItem createItem(ReconciliationRun run, ReconciliationItemDraft draft) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_reconciliation_items (
                    workspace_id, reconciliation_run_id, truth_import_id, provider_profile_id, currency_code, source_reference,
                    mismatch_type,
                    internal_settled_gross_amount_minor, provider_settled_gross_amount_minor,
                    internal_fee_amount_minor, provider_fee_amount_minor,
                    internal_net_earnings_amount_minor, provider_net_earnings_amount_minor,
                    internal_payout_succeeded_amount_minor, provider_payout_succeeded_amount_minor,
                    internal_refund_amount_minor, provider_refund_amount_minor,
                    internal_active_dispute_exposure_amount_minor, provider_active_dispute_exposure_amount_minor,
                    internal_settled_subscription_net_revenue_amount_minor, provider_settled_subscription_net_revenue_amount_minor,
                    detail_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                RETURNING *, detail_json::text
                """, ITEM_MAPPER,
                run.workspaceId(), run.id(), run.truthImportId(), run.providerProfileId(), run.currencyCode(), run.sourceReference(),
                draft.mismatchType(),
                draft.internalSettledGrossAmountMinor(), draft.providerSettledGrossAmountMinor(),
                draft.internalFeeAmountMinor(), draft.providerFeeAmountMinor(),
                draft.internalNetEarningsAmountMinor(), draft.providerNetEarningsAmountMinor(),
                draft.internalPayoutSucceededAmountMinor(), draft.providerPayoutSucceededAmountMinor(),
                draft.internalRefundAmountMinor(), draft.providerRefundAmountMinor(),
                draft.internalActiveDisputeExposureAmountMinor(), draft.providerActiveDisputeExposureAmountMinor(),
                draft.internalSettledSubscriptionNetRevenueAmountMinor(), draft.providerSettledSubscriptionNetRevenueAmountMinor(),
                draft.detailJson());
    }

    public ReconciliationRun completeRun(UUID workspaceId, UUID runId, int unresolvedItemCount) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_reconciliation_runs
                SET status = CASE WHEN ? = 0 THEN 'MATCHED' ELSE 'MISMATCHED' END,
                    unresolved_item_count = ?,
                    completed_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, RUN_MAPPER, unresolvedItemCount, unresolvedItemCount, workspaceId, runId);
    }

    public void markImportReconciled(UUID workspaceId, UUID truthImportId) {
        jdbcTemplate.update("""
                UPDATE provider_reconciliation_truth_imports
                SET status = 'RECONCILED'
                WHERE workspace_id = ?
                  AND id = ?
                """, workspaceId, truthImportId);
    }

    public List<ReconciliationRun> listRuns(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_reconciliation_runs
                WHERE workspace_id = ?
                ORDER BY started_at DESC, id
                """, RUN_MAPPER, workspaceId);
    }

    public Optional<ReconciliationRun> findRun(UUID workspaceId, UUID runId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_reconciliation_runs
                WHERE workspace_id = ?
                  AND id = ?
                """, RUN_MAPPER, workspaceId, runId).stream().findFirst();
    }

    public List<ReconciliationItem> listItems(UUID workspaceId, UUID runId, boolean unresolvedOnly) {
        return jdbcTemplate.query("""
                SELECT *, detail_json::text
                FROM provider_reconciliation_items
                WHERE workspace_id = ?
                  AND reconciliation_run_id = ?
                  AND (? = false OR status = 'OPEN')
                ORDER BY created_at, id
                """, ITEM_MAPPER, workspaceId, runId, unresolvedOnly);
    }

    public Optional<ReconciliationItem> findItem(UUID workspaceId, UUID itemId) {
        return jdbcTemplate.query("""
                SELECT *, detail_json::text
                FROM provider_reconciliation_items
                WHERE workspace_id = ?
                  AND id = ?
                """, ITEM_MAPPER, workspaceId, itemId).stream().findFirst();
    }

    public ReconciliationItem resolveItem(UUID workspaceId, UUID itemId, UUID actorId, String resolutionOutcome, String resolutionNote) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_reconciliation_items
                SET status = 'RESOLVED',
                    resolution_outcome = ?,
                    resolution_note = ?,
                    resolved_by_actor_id = ?,
                    resolved_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'OPEN'
                RETURNING *, detail_json::text
                """, ITEM_MAPPER, resolutionOutcome, resolutionNote, actorId, workspaceId, itemId);
    }

    public ReconciliationItem reopenItem(UUID workspaceId, UUID itemId) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_reconciliation_items
                SET status = 'OPEN',
                    resolution_outcome = NULL,
                    resolution_note = NULL,
                    resolved_by_actor_id = NULL,
                    resolved_at = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'RESOLVED'
                RETURNING *, detail_json::text
                """, ITEM_MAPPER, workspaceId, itemId);
    }

    public void refreshRunCounts(UUID workspaceId, UUID runId) {
        jdbcTemplate.update("""
                UPDATE provider_reconciliation_runs
                SET unresolved_item_count = (
                        SELECT count(*)
                        FROM provider_reconciliation_items
                        WHERE workspace_id = ?
                          AND reconciliation_run_id = ?
                          AND status = 'OPEN'
                    ),
                    resolved_item_count = (
                        SELECT count(*)
                        FROM provider_reconciliation_items
                        WHERE workspace_id = ?
                          AND reconciliation_run_id = ?
                          AND status = 'RESOLVED'
                    )
                WHERE workspace_id = ?
                  AND id = ?
                """, workspaceId, runId, workspaceId, runId, workspaceId, runId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    public record ProviderTruthSnapshotDraft(
            long settledGrossAmountMinor,
            long feeAmountMinor,
            long netEarningsAmountMinor,
            long payoutSucceededAmountMinor,
            long refundAmountMinor,
            long activeDisputeExposureAmountMinor,
            long settledSubscriptionNetRevenueAmountMinor,
            String providerPayloadJson) {
    }

    public record ReconciliationItemDraft(
            String mismatchType,
            Long internalSettledGrossAmountMinor,
            Long providerSettledGrossAmountMinor,
            Long internalFeeAmountMinor,
            Long providerFeeAmountMinor,
            Long internalNetEarningsAmountMinor,
            Long providerNetEarningsAmountMinor,
            Long internalPayoutSucceededAmountMinor,
            Long providerPayoutSucceededAmountMinor,
            Long internalRefundAmountMinor,
            Long providerRefundAmountMinor,
            Long internalActiveDisputeExposureAmountMinor,
            Long providerActiveDisputeExposureAmountMinor,
            Long internalSettledSubscriptionNetRevenueAmountMinor,
            Long providerSettledSubscriptionNetRevenueAmountMinor,
            String detailJson) {
    }
}
