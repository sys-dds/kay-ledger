package com.kayledger.api.reconciliation.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.reconciliation.model.ReconciliationMismatch;
import com.kayledger.api.reconciliation.model.ReconciliationRun;

@Repository
public class ReconciliationStore {

    private static final RowMapper<ReconciliationRun> RUN_MAPPER = (rs, rowNum) -> new ReconciliationRun(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_config_id", UUID.class),
            rs.getString("run_type"),
            rs.getString("status"),
            instant(rs, "started_at"),
            nullableInstant(rs, "completed_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ReconciliationMismatch> MISMATCH_MAPPER = (rs, rowNum) -> new ReconciliationMismatch(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("reconciliation_run_id", UUID.class),
            rs.getObject("provider_callback_id", UUID.class),
            rs.getString("business_reference_type"),
            rs.getObject("business_reference_id", UUID.class),
            rs.getString("drift_category"),
            rs.getString("internal_state"),
            rs.getString("provider_state"),
            rs.getString("suggested_action"),
            rs.getString("repair_status"),
            rs.getString("repair_note"),
            nullableInstant(rs, "repaired_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public ReconciliationStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ReconciliationRun createRun(UUID workspaceId, String runType) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO reconciliation_runs (workspace_id, run_type)
                VALUES (?, ?)
                RETURNING *
                """, RUN_MAPPER, workspaceId, runType);
    }

    public ReconciliationRun completeRun(UUID workspaceId, UUID runId) {
        return jdbcTemplate.queryForObject("""
                UPDATE reconciliation_runs
                SET status = 'COMPLETED',
                    completed_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, RUN_MAPPER, workspaceId, runId);
    }

    public List<ReconciliationRun> listRuns(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM reconciliation_runs
                WHERE workspace_id = ?
                ORDER BY started_at DESC, id
                """, RUN_MAPPER, workspaceId);
    }

    public ReconciliationMismatch createMismatch(
            UUID workspaceId,
            UUID runId,
            UUID providerCallbackId,
            String referenceType,
            UUID referenceId,
            String driftCategory,
            String internalState,
            String providerState,
            String suggestedAction) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO reconciliation_mismatches (
                    workspace_id, reconciliation_run_id, provider_callback_id,
                    business_reference_type, business_reference_id, drift_category,
                    internal_state, provider_state, suggested_action
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, MISMATCH_MAPPER, workspaceId, runId, providerCallbackId, referenceType, referenceId, driftCategory, internalState, providerState, suggestedAction);
    }

    public int createEntityCentricMismatches(UUID workspaceId, UUID runId) {
        Integer failedCallbacks = jdbcTemplate.queryForObject("""
                WITH inserted AS (
                    INSERT INTO reconciliation_mismatches (
                        workspace_id, reconciliation_run_id, provider_callback_id,
                        business_reference_type, business_reference_id, drift_category,
                        internal_state, provider_state, suggested_action
                    )
                    SELECT workspace_id, ?, id, business_reference_type, business_reference_id,
                           'FAILED_CALLBACK_BACKLOG', processing_status, callback_type, 'MANUAL_REVIEW'
                    FROM provider_callbacks
                    WHERE workspace_id = ?
                      AND processing_status = 'FAILED'
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """, Integer.class, runId, workspaceId);
        Integer paymentProjectionDrift = jdbcTemplate.queryForObject("""
                WITH inserted AS (
                    INSERT INTO reconciliation_mismatches (
                        workspace_id, reconciliation_run_id, provider_callback_id,
                        business_reference_type, business_reference_id, drift_category,
                        internal_state, provider_state, suggested_action
                    )
                    SELECT pi.workspace_id, ?, NULL, 'PAYMENT_INTENT', pi.id,
                           CASE WHEN pp.payment_intent_id IS NULL THEN 'MISSING_PROJECTION' ELSE 'STATE_MISMATCH' END,
                           pi.status, COALESCE(pp.latest_payment_status, 'MISSING_PROJECTION'), 'MANUAL_REVIEW'
                    FROM payment_intents pi
                    LEFT JOIN payment_projection pp ON pp.workspace_id = pi.workspace_id
                     AND pp.payment_intent_id = pi.id
                    WHERE pi.workspace_id = ?
                      AND (pp.payment_intent_id IS NULL OR pp.latest_payment_status <> pi.status)
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """, Integer.class, runId, workspaceId);
        Integer paymentJournalDrift = jdbcTemplate.queryForObject("""
                WITH inserted AS (
                    INSERT INTO reconciliation_mismatches (
                        workspace_id, reconciliation_run_id, provider_callback_id,
                        business_reference_type, business_reference_id, drift_category,
                        internal_state, provider_state, suggested_action
                    )
                    SELECT pi.workspace_id, ?, NULL, 'PAYMENT_INTENT', pi.id,
                           'MISSING_JOURNAL', pi.status, 'MISSING_SETTLEMENT_JOURNAL', 'MANUAL_REVIEW'
                    FROM payment_intents pi
                    WHERE pi.workspace_id = ?
                      AND pi.status = 'SETTLED'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM payment_attempts pa
                          WHERE pa.workspace_id = pi.workspace_id
                            AND pa.payment_intent_id = pi.id
                            AND pa.attempt_type = 'SETTLE'
                            AND pa.status = 'SUCCEEDED'
                            AND pa.journal_entry_id IS NOT NULL
                      )
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """, Integer.class, runId, workspaceId);
        Integer refundJournalDrift = jdbcTemplate.queryForObject("""
                WITH inserted AS (
                    INSERT INTO reconciliation_mismatches (
                        workspace_id, reconciliation_run_id, provider_callback_id,
                        business_reference_type, business_reference_id, drift_category,
                        internal_state, provider_state, suggested_action
                    )
                    SELECT workspace_id, ?, NULL, 'REFUND', id,
                           'MISSING_JOURNAL', status, 'MISSING_REFUND_JOURNAL', 'MANUAL_REVIEW'
                    FROM refunds
                    WHERE workspace_id = ?
                      AND status = 'SUCCEEDED'
                      AND journal_entry_id IS NULL
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """, Integer.class, runId, workspaceId);
        return count(failedCallbacks) + count(paymentProjectionDrift) + count(paymentJournalDrift) + count(refundJournalDrift);
    }

    public List<ReconciliationMismatch> listMismatches(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM reconciliation_mismatches
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, MISMATCH_MAPPER, workspaceId);
    }

    public Optional<ReconciliationMismatch> findMismatch(UUID workspaceId, UUID mismatchId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM reconciliation_mismatches
                WHERE workspace_id = ?
                  AND id = ?
                """, MISMATCH_MAPPER, workspaceId, mismatchId).stream().findFirst();
    }

    public ReconciliationMismatch markRepair(UUID workspaceId, UUID mismatchId, String note) {
        return jdbcTemplate.queryForObject("""
                UPDATE reconciliation_mismatches
                SET repair_status = 'MARKED',
                    repair_note = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND repair_status = 'OPEN'
                RETURNING *
                """, MISMATCH_MAPPER, note, workspaceId, mismatchId);
    }

    public ReconciliationMismatch markApplied(UUID workspaceId, UUID mismatchId, String note) {
        return jdbcTemplate.queryForObject("""
                UPDATE reconciliation_mismatches
                SET repair_status = 'APPLIED',
                    repair_note = ?,
                    repaired_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                  AND repair_status IN ('OPEN', 'MARKED')
                RETURNING *
                """, MISMATCH_MAPPER, note, workspaceId, mismatchId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static int count(Integer value) {
        return value == null ? 0 : value;
    }
}
