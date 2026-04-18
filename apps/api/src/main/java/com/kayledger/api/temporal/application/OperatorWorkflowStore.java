package com.kayledger.api.temporal.application;

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

@Repository
public class OperatorWorkflowStore {

    private static final RowMapper<OperatorWorkflowRecord> MAPPER = (rs, rowNum) -> new OperatorWorkflowRecord(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("workflow_type"),
            rs.getString("business_reference_type"),
            rs.getObject("business_reference_id", UUID.class),
            rs.getString("workflow_id"),
            rs.getString("run_id"),
            rs.getString("trigger_mode"),
            rs.getString("status"),
            rs.getInt("progress_current"),
            rs.getInt("progress_total"),
            rs.getString("progress_message"),
            rs.getObject("requested_by_actor_id", UUID.class),
            instant(rs, "started_at"),
            nullableInstant(rs, "completed_at"),
            rs.getString("failure_reason"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public OperatorWorkflowStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public OperatorWorkflowRecord createRequested(
            UUID workspaceId,
            String workflowType,
            String businessReferenceType,
            UUID businessReferenceId,
            String workflowId,
            String triggerMode,
            UUID requestedByActorId,
            int progressTotal,
            String progressMessage) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO operator_workflows (
                    workspace_id, workflow_type, business_reference_type, business_reference_id,
                    workflow_id, trigger_mode, requested_by_actor_id, progress_total, progress_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, MAPPER, workspaceId, workflowType, businessReferenceType, businessReferenceId,
                workflowId, triggerMode, requestedByActorId, progressTotal, progressMessage);
    }

    public OperatorWorkflowRecord attachRun(UUID workspaceId, String workflowId, String runId) {
        return jdbcTemplate.queryForObject("""
                UPDATE operator_workflows
                SET run_id = ?
                WHERE workspace_id = ?
                  AND workflow_id = ?
                RETURNING *
                """, MAPPER, runId, workspaceId, workflowId);
    }

    public OperatorWorkflowRecord markRunning(UUID workspaceId, String workflowId, String progressMessage) {
        return jdbcTemplate.queryForObject("""
                UPDATE operator_workflows
                SET status = 'RUNNING',
                    progress_message = ?,
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND workflow_id = ?
                  AND status IN ('REQUESTED', 'RUNNING')
                RETURNING *
                """, MAPPER, progressMessage, workspaceId, workflowId);
    }

    public OperatorWorkflowRecord markProgress(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        return jdbcTemplate.queryForObject("""
                UPDATE operator_workflows
                SET progress_current = ?,
                    progress_total = GREATEST(progress_total, ?),
                    progress_message = ?
                WHERE workspace_id = ?
                  AND workflow_id = ?
                  AND status IN ('REQUESTED', 'RUNNING')
                RETURNING *
                """, MAPPER, progressCurrent, progressTotal, progressMessage, workspaceId, workflowId);
    }

    public OperatorWorkflowRecord markSucceeded(UUID workspaceId, String workflowId, int progressCurrent, int progressTotal, String progressMessage) {
        return jdbcTemplate.queryForObject("""
                UPDATE operator_workflows
                SET status = 'SUCCEEDED',
                    progress_current = ?,
                    progress_total = ?,
                    progress_message = ?,
                    completed_at = now(),
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND workflow_id = ?
                RETURNING *
                """, MAPPER, progressCurrent, progressTotal, progressMessage, workspaceId, workflowId);
    }

    public OperatorWorkflowRecord markFailed(UUID workspaceId, String workflowId, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE operator_workflows
                SET status = 'FAILED',
                    completed_at = now(),
                    failure_reason = ?
                WHERE workspace_id = ?
                  AND workflow_id = ?
                RETURNING *
                """, MAPPER, truncate(failureReason), workspaceId, workflowId);
    }

    public Optional<OperatorWorkflowRecord> find(UUID workspaceId, String workflowId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM operator_workflows
                WHERE workspace_id = ?
                  AND workflow_id = ?
                """, MAPPER, workspaceId, workflowId).stream().findFirst();
    }

    public Optional<OperatorWorkflowRecord> findByReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM operator_workflows
                WHERE workspace_id = ?
                  AND business_reference_type = ?
                  AND business_reference_id = ?
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """, MAPPER, workspaceId, referenceType, referenceId).stream().findFirst();
    }

    public List<OperatorWorkflowRecord> list(UUID workspaceId, String workflowType) {
        return jdbcTemplate.query("""
                SELECT *
                FROM operator_workflows
                WHERE workspace_id = ?
                  AND workflow_type = ?
                ORDER BY started_at DESC, id DESC
                """, MAPPER, workspaceId, workflowType);
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
}
