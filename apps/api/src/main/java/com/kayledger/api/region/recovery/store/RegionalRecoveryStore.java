package com.kayledger.api.region.recovery.store;

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

import com.kayledger.api.region.recovery.model.RegionalDriftRecord;
import com.kayledger.api.region.recovery.model.RegionalRecoveryAction;

@Repository
public class RegionalRecoveryStore {

    private static final RowMapper<RegionalDriftRecord> DRIFT_MAPPER = (rs, rowNum) -> new RegionalDriftRecord(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("drift_type"),
            rs.getString("source_region"),
            rs.getString("target_region"),
            rs.getString("reference_type"),
            rs.getString("reference_id"),
            rs.getString("status"),
            rs.getString("detail_json"),
            instant(rs, "detected_at"),
            nullableInstant(rs, "resolved_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<RegionalRecoveryAction> ACTION_MAPPER = (rs, rowNum) -> new RegionalRecoveryAction(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("drift_record_id", UUID.class),
            rs.getString("action_type"),
            rs.getString("reference_type"),
            rs.getString("reference_id"),
            rs.getString("status"),
            rs.getObject("requested_by_actor_id", UUID.class),
            rs.getString("result_json"),
            rs.getString("failure_reason"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"),
            nullableInstant(rs, "completed_at"));

    private final JdbcTemplate jdbcTemplate;

    public RegionalRecoveryStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RegionalDriftRecord upsertOpenDrift(UUID workspaceId, String driftType, String sourceRegion, String targetRegion, String referenceType, String referenceId, String detailJson) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO regional_drift_records (
                    workspace_id, drift_type, source_region, target_region, reference_type, reference_id, detail_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (workspace_id, drift_type, reference_type, reference_id, status) DO UPDATE
                SET source_region = EXCLUDED.source_region,
                    target_region = EXCLUDED.target_region,
                    detail_json = EXCLUDED.detail_json
                RETURNING id, workspace_id, drift_type, source_region, target_region, reference_type, reference_id,
                          status, detail_json::text, detected_at, resolved_at, created_at, updated_at
                """, DRIFT_MAPPER, workspaceId, driftType, sourceRegion, targetRegion, referenceType, referenceId, detailJson);
    }

    public List<RegionalDriftRecord> listDrift(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, drift_type, source_region, target_region, reference_type, reference_id,
                       status, detail_json::text, detected_at, resolved_at, created_at, updated_at
                FROM regional_drift_records
                WHERE workspace_id = ?
                ORDER BY detected_at DESC, id
                """, DRIFT_MAPPER, workspaceId);
    }

    public List<RegionalDriftRecord> listUnresolvedDrift(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, drift_type, source_region, target_region, reference_type, reference_id,
                       status, detail_json::text, detected_at, resolved_at, created_at, updated_at
                FROM regional_drift_records
                WHERE workspace_id = ?
                  AND status = 'OPEN'
                ORDER BY detected_at DESC, id
                """, DRIFT_MAPPER, workspaceId);
    }

    public Optional<RegionalDriftRecord> findDrift(UUID workspaceId, UUID driftRecordId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, drift_type, source_region, target_region, reference_type, reference_id,
                       status, detail_json::text, detected_at, resolved_at, created_at, updated_at
                FROM regional_drift_records
                WHERE workspace_id = ?
                  AND id = ?
                """, DRIFT_MAPPER, workspaceId, driftRecordId).stream().findFirst();
    }

    public void resolveDrift(UUID workspaceId, UUID driftRecordId) {
        jdbcTemplate.update("""
                UPDATE regional_drift_records
                SET status = 'RESOLVED',
                    resolved_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'OPEN'
                """, workspaceId, driftRecordId);
    }

    public RegionalRecoveryAction createAction(UUID workspaceId, UUID driftRecordId, String actionType, String referenceType, String referenceId, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO regional_recovery_actions (
                    workspace_id, drift_record_id, action_type, reference_type, reference_id, requested_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING id, workspace_id, drift_record_id, action_type, reference_type, reference_id, status,
                          requested_by_actor_id, result_json::text, failure_reason, created_at, updated_at, completed_at
                """, ACTION_MAPPER, workspaceId, driftRecordId, actionType, referenceType, referenceId, actorId);
    }

    public RegionalRecoveryAction markActionSucceeded(UUID workspaceId, UUID actionId, String resultJson) {
        return jdbcTemplate.queryForObject("""
                UPDATE regional_recovery_actions
                SET status = 'SUCCEEDED',
                    result_json = ?::jsonb,
                    failure_reason = NULL,
                    completed_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING id, workspace_id, drift_record_id, action_type, reference_type, reference_id, status,
                          requested_by_actor_id, result_json::text, failure_reason, created_at, updated_at, completed_at
                """, ACTION_MAPPER, resultJson, workspaceId, actionId);
    }

    public RegionalRecoveryAction markActionFailed(UUID workspaceId, UUID actionId, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE regional_recovery_actions
                SET status = 'FAILED',
                    failure_reason = ?,
                    completed_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING id, workspace_id, drift_record_id, action_type, reference_type, reference_id, status,
                          requested_by_actor_id, result_json::text, failure_reason, created_at, updated_at, completed_at
                """, ACTION_MAPPER, truncate(failureReason), workspaceId, actionId);
    }

    public List<RegionalRecoveryAction> listActions(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, drift_record_id, action_type, reference_type, reference_id, status,
                       requested_by_actor_id, result_json::text, failure_reason, created_at, updated_at, completed_at
                FROM regional_recovery_actions
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, ACTION_MAPPER, workspaceId);
    }

    public List<DelayedCallbackRow> delayedCallbacks(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id
                FROM provider_callbacks
                WHERE workspace_id = ?
                  AND processing_status = 'DELAYED_BY_DRILL'
                ORDER BY received_at, id
                """, (rs, rowNum) -> new DelayedCallbackRow(rs.getObject("id", UUID.class)), workspaceId);
    }

    public List<SnapshotCheckpointDriftRow> snapshotCheckpointDrifts(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT c.stream_name, c.source_region, c.target_region, c.last_applied_sequence
                FROM region_replication_checkpoints c
                WHERE c.workspace_id = ?
                  AND c.stream_name IN ('INVESTIGATION_READ_SNAPSHOT', 'PROVIDER_SUMMARY_SNAPSHOT')
                  AND (
                    (c.stream_name = 'INVESTIGATION_READ_SNAPSHOT' AND NOT EXISTS (
                        SELECT 1 FROM region_investigation_read_snapshots s
                        WHERE s.workspace_id = c.workspace_id
                          AND s.target_region = c.target_region
                          AND s.replication_sequence = c.last_applied_sequence
                    ))
                    OR
                    (c.stream_name = 'PROVIDER_SUMMARY_SNAPSHOT' AND NOT EXISTS (
                        SELECT 1 FROM region_provider_summary_snapshots s
                        WHERE s.workspace_id = c.workspace_id
                          AND s.target_region = c.target_region
                          AND s.replication_sequence = c.last_applied_sequence
                    ))
                  )
                """, (rs, rowNum) -> new SnapshotCheckpointDriftRow(
                rs.getString("stream_name"),
                rs.getString("source_region"),
                rs.getString("target_region"),
                rs.getLong("last_applied_sequence")), workspaceId);
    }

    public boolean hasOwnershipTransferCheckpoint(UUID workspaceId) {
        Boolean exists = jdbcTemplate.queryForObject("""
                SELECT EXISTS (
                    SELECT 1
                    FROM region_replication_checkpoints
                    WHERE workspace_id = ?
                      AND stream_name = 'WORKSPACE_OWNERSHIP_TRANSFER'
                )
                """, Boolean.class, workspaceId);
        return Boolean.TRUE.equals(exists);
    }

    public boolean hasFailoverHistory(UUID workspaceId) {
        Boolean exists = jdbcTemplate.queryForObject("""
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_region_failover_events
                    WHERE workspace_id = ?
                )
                """, Boolean.class, workspaceId);
        return Boolean.TRUE.equals(exists);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }

    public record DelayedCallbackRow(UUID callbackId) {
    }

    public record SnapshotCheckpointDriftRow(String streamName, String sourceRegion, String targetRegion, long lastAppliedSequence) {
    }
}
