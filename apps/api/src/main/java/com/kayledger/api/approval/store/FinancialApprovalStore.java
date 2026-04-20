package com.kayledger.api.approval.store;

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

import com.kayledger.api.approval.model.FinancialApprovalDecision;
import com.kayledger.api.approval.model.FinancialApprovalExecutionState;
import com.kayledger.api.approval.model.FinancialApprovalRequest;

@Repository
public class FinancialApprovalStore {

    private static final RowMapper<FinancialApprovalRequest> REQUEST_MAPPER = (rs, rowNum) -> new FinancialApprovalRequest(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("action_type"),
            rs.getString("target_type"),
            rs.getObject("target_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            nullableLong(rs, "amount_minor"),
            rs.getString("status"),
            rs.getObject("requested_by_actor_id", UUID.class),
            rs.getString("reason"),
            rs.getString("request_payload_json"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FinancialApprovalDecision> DECISION_MAPPER = (rs, rowNum) -> new FinancialApprovalDecision(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("approval_request_id", UUID.class),
            rs.getString("decision"),
            rs.getObject("decided_by_actor_id", UUID.class),
            rs.getString("note"),
            instant(rs, "created_at"));

    private static final RowMapper<FinancialApprovalExecutionState> EXECUTION_MAPPER = (rs, rowNum) -> new FinancialApprovalExecutionState(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("approval_request_id", UUID.class),
            rs.getString("execution_status"),
            rs.getObject("executed_by_actor_id", UUID.class),
            nullableInstant(rs, "started_at"),
            nullableInstant(rs, "last_attempt_at"),
            rs.getInt("execution_attempt_count"),
            nullableInstant(rs, "executed_at"),
            rs.getString("failure_reason"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public FinancialApprovalStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public FinancialApprovalRequest createRequest(UUID workspaceId, String actionType, String targetType, UUID targetId, UUID providerProfileId, String currencyCode, Long amountMinor, UUID requestedByActorId, String reason, String payloadJson) {
        FinancialApprovalRequest request = jdbcTemplate.queryForObject("""
                INSERT INTO financial_approval_requests (
                    workspace_id, action_type, target_type, target_id, provider_profile_id, currency_code,
                    amount_minor, requested_by_actor_id, reason, request_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                RETURNING *, request_payload_json::text
                """, REQUEST_MAPPER, workspaceId, actionType, targetType, targetId, providerProfileId, currencyCode, amountMinor, requestedByActorId, reason, payloadJson);
        jdbcTemplate.update("""
                INSERT INTO financial_approval_execution_state (workspace_id, approval_request_id, execution_status)
                VALUES (?, ?, 'PENDING')
                """, workspaceId, request.id());
        return request;
    }

    public List<FinancialApprovalRequest> listRequests(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *, request_payload_json::text
                FROM financial_approval_requests
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, REQUEST_MAPPER, workspaceId);
    }

    public Optional<FinancialApprovalRequest> findRequest(UUID workspaceId, UUID requestId) {
        return jdbcTemplate.query("""
                SELECT *, request_payload_json::text
                FROM financial_approval_requests
                WHERE workspace_id = ?
                  AND id = ?
                """, REQUEST_MAPPER, workspaceId, requestId).stream().findFirst();
    }

    public FinancialApprovalDecision createDecision(UUID workspaceId, UUID requestId, String decision, UUID actorId, String note) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO financial_approval_decisions (workspace_id, approval_request_id, decision, decided_by_actor_id, note)
                VALUES (?, ?, ?, ?, ?)
                RETURNING *
                """, DECISION_MAPPER, workspaceId, requestId, decision, actorId, note);
    }

    public FinancialApprovalRequest updateStatus(UUID workspaceId, UUID requestId, String status) {
        return jdbcTemplate.queryForObject("""
                UPDATE financial_approval_requests
                SET status = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'REQUESTED'
                RETURNING *, request_payload_json::text
                """, REQUEST_MAPPER, status, workspaceId, requestId);
    }

    public FinancialApprovalExecutionState markExecutionInProgress(UUID workspaceId, UUID requestId, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                UPDATE financial_approval_execution_state execution
                SET execution_status = 'IN_PROGRESS',
                    executed_by_actor_id = ?,
                    started_at = COALESCE(started_at, now()),
                    last_attempt_at = now(),
                    execution_attempt_count = execution_attempt_count + 1,
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                  AND execution_status IN ('PENDING', 'FAILED', 'BLOCKED')
                  AND EXISTS (
                      SELECT 1
                      FROM financial_approval_requests request
                      WHERE request.workspace_id = execution.workspace_id
                        AND request.id = execution.approval_request_id
                        AND request.status = 'APPROVED'
                  )
                RETURNING *
                """, EXECUTION_MAPPER, actorId, workspaceId, requestId);
    }

    public FinancialApprovalRequest markExecuted(UUID workspaceId, UUID requestId, UUID actorId) {
        jdbcTemplate.update("""
                UPDATE financial_approval_execution_state
                SET execution_status = 'EXECUTED',
                    executed_by_actor_id = ?,
                    executed_at = now(),
                    last_attempt_at = now(),
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                  AND execution_status = 'IN_PROGRESS'
                """, actorId, workspaceId, requestId);
        return jdbcTemplate.queryForObject("""
                UPDATE financial_approval_requests
                SET status = 'EXECUTED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'APPROVED'
                RETURNING *, request_payload_json::text
                """, REQUEST_MAPPER, workspaceId, requestId);
    }

    public void markExecutionFailed(UUID workspaceId, UUID requestId, String reason) {
        jdbcTemplate.update("""
                UPDATE financial_approval_execution_state
                SET execution_status = 'FAILED',
                    last_attempt_at = now(),
                    failure_reason = ?
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                  AND execution_status <> 'EXECUTED'
                """, reason, workspaceId, requestId);
    }

    public void markBlocked(UUID workspaceId, UUID requestId, String reason) {
        jdbcTemplate.update("""
                UPDATE financial_approval_execution_state
                SET execution_status = 'BLOCKED',
                    failure_reason = ?
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                  AND execution_status <> 'EXECUTED'
                """, reason, workspaceId, requestId);
    }

    public List<FinancialApprovalDecision> listDecisions(UUID workspaceId, UUID requestId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_approval_decisions
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                ORDER BY created_at, id
                """, DECISION_MAPPER, workspaceId, requestId);
    }

    public Optional<FinancialApprovalExecutionState> executionState(UUID workspaceId, UUID requestId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_approval_execution_state
                WHERE workspace_id = ?
                  AND approval_request_id = ?
                """, EXECUTION_MAPPER, workspaceId, requestId).stream().findFirst();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }
}
