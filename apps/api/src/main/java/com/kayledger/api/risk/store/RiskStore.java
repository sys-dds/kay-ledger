package com.kayledger.api.risk.store;

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

import com.kayledger.api.risk.model.RiskDecision;
import com.kayledger.api.risk.model.RiskFlag;
import com.kayledger.api.risk.model.RiskReview;

@Repository
public class RiskStore {

    private static final RowMapper<RiskFlag> FLAG_MAPPER = (rs, rowNum) -> new RiskFlag(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("reference_type"),
            rs.getObject("reference_id", UUID.class),
            rs.getString("rule_code"),
            rs.getString("severity"),
            rs.getString("status"),
            rs.getString("reason"),
            rs.getInt("signal_count"),
            instant(rs, "first_seen_at"),
            instant(rs, "last_seen_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<RiskReview> REVIEW_MAPPER = (rs, rowNum) -> new RiskReview(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("risk_flag_id", UUID.class),
            rs.getString("status"),
            rs.getObject("assigned_actor_id", UUID.class),
            instant(rs, "opened_at"),
            nullableInstant(rs, "closed_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<RiskDecision> DECISION_MAPPER = (rs, rowNum) -> new RiskDecision(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("risk_flag_id", UUID.class),
            rs.getObject("review_id", UUID.class),
            rs.getString("reference_type"),
            rs.getObject("reference_id", UUID.class),
            rs.getString("outcome"),
            rs.getString("reason"),
            rs.getObject("decided_by_actor_id", UUID.class),
            instant(rs, "decided_at"),
            instant(rs, "created_at"));

    private final JdbcTemplate jdbcTemplate;

    public RiskStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RiskFlag upsertFlag(UUID workspaceId, String referenceType, UUID referenceId, String ruleCode, String severity, String reason, int signalCount) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO risk_flags (
                    workspace_id, reference_type, reference_id, rule_code, severity, reason, signal_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, reference_type, reference_id, rule_code) DO UPDATE
                SET severity = EXCLUDED.severity,
                    reason = EXCLUDED.reason,
                    signal_count = GREATEST(risk_flags.signal_count, EXCLUDED.signal_count),
                    last_seen_at = now(),
                    status = CASE WHEN risk_flags.status IN ('RESOLVED', 'DISMISSED') THEN 'OPEN' ELSE risk_flags.status END
                RETURNING *
                """, FLAG_MAPPER, workspaceId, referenceType, referenceId, ruleCode, severity, reason, signalCount);
    }

    public RiskReview ensureReview(UUID workspaceId, UUID riskFlagId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO risk_reviews (workspace_id, risk_flag_id)
                VALUES (?, ?)
                ON CONFLICT (workspace_id, risk_flag_id) DO UPDATE
                SET status = CASE WHEN risk_reviews.status IN ('RESOLVED', 'DISMISSED') THEN 'OPEN' ELSE risk_reviews.status END,
                    closed_at = CASE WHEN risk_reviews.status IN ('RESOLVED', 'DISMISSED') THEN NULL ELSE risk_reviews.closed_at END
                RETURNING *
                """, REVIEW_MAPPER, workspaceId, riskFlagId);
    }

    public List<RiskFlag> listFlags(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM risk_flags
                WHERE workspace_id = ?
                ORDER BY updated_at DESC, id
                """, FLAG_MAPPER, workspaceId);
    }

    public List<RiskReview> listReviewQueue(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM risk_reviews
                WHERE workspace_id = ?
                  AND status IN ('OPEN', 'IN_REVIEW', 'BLOCKED')
                ORDER BY updated_at DESC, id
                """, REVIEW_MAPPER, workspaceId);
    }

    public RiskReview markInReview(UUID workspaceId, UUID reviewId, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                UPDATE risk_reviews
                SET status = 'IN_REVIEW',
                    assigned_actor_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'OPEN'
                RETURNING *
                """, REVIEW_MAPPER, actorId, workspaceId, reviewId);
    }

    public RiskDecision decide(UUID workspaceId, UUID reviewId, String outcome, String reason, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                WITH review AS (
                    SELECT rr.*, rf.reference_type, rf.reference_id
                    FROM risk_reviews rr
                    JOIN risk_flags rf ON rf.workspace_id = rr.workspace_id AND rf.id = rr.risk_flag_id
                    WHERE rr.workspace_id = ?
                      AND rr.id = ?
                ), inserted AS (
                    INSERT INTO risk_decisions (
                        workspace_id, risk_flag_id, review_id, reference_type, reference_id,
                        outcome, reason, decided_by_actor_id
                    )
                    SELECT workspace_id, risk_flag_id, id, reference_type, reference_id, ?, ?, ?
                    FROM review
                    RETURNING *
                ), review_update AS (
                    UPDATE risk_reviews rr
                    SET status = CASE
                            WHEN ? = 'REVIEW' THEN 'IN_REVIEW'
                            WHEN ? = 'BLOCK' THEN 'BLOCKED'
                            ELSE 'RESOLVED'
                        END,
                        closed_at = CASE WHEN ? IN ('REVIEW', 'BLOCK') THEN NULL ELSE now() END
                    FROM inserted i
                    WHERE rr.workspace_id = i.workspace_id
                      AND rr.id = i.review_id
                    RETURNING rr.risk_flag_id
                )
                UPDATE risk_flags rf
                SET status = CASE
                        WHEN ? = 'REVIEW' THEN 'IN_REVIEW'
                        WHEN ? = 'BLOCK' THEN 'BLOCKED'
                        ELSE 'RESOLVED'
                    END
                FROM inserted i
                WHERE rf.workspace_id = i.workspace_id
                  AND rf.id = i.risk_flag_id
                RETURNING i.*
                """, DECISION_MAPPER, workspaceId, reviewId, outcome, reason, actorId, outcome, outcome, outcome, outcome, outcome);
    }

    public List<RiskDecision> listDecisions(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM risk_decisions
                WHERE workspace_id = ?
                ORDER BY decided_at DESC, id
                """, DECISION_MAPPER, workspaceId);
    }

    public Optional<RiskDecision> latestBlockingDecision(UUID workspaceId, String referenceType, UUID referenceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM risk_decisions
                WHERE workspace_id = ?
                  AND reference_type = ?
                  AND reference_id = ?
                ORDER BY decided_at DESC, id DESC
                LIMIT 1
                """, DECISION_MAPPER, workspaceId, referenceType, referenceId).stream()
                .filter(decision -> "BLOCK".equals(decision.outcome()))
                .findFirst();
    }

    public int failedPaymentCountForProvider(UUID workspaceId, UUID providerProfileId) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM payment_intents
                WHERE workspace_id = ?
                  AND provider_profile_id = ?
                  AND status = 'FAILED'
                  AND updated_at >= now() - interval '1 day'
                """, Integer.class, workspaceId, providerProfileId);
        return value == null ? 0 : value;
    }

    public int refundCountForProvider(UUID workspaceId, UUID providerProfileId) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM refunds r
                JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
                WHERE r.workspace_id = ?
                  AND pi.provider_profile_id = ?
                  AND r.created_at >= now() - interval '1 day'
                """, Integer.class, workspaceId, providerProfileId);
        return value == null ? 0 : value;
    }

    public int openMismatchCount(UUID workspaceId) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT COUNT(*)
                FROM reconciliation_mismatches
                WHERE workspace_id = ?
                  AND repair_status = 'OPEN'
                  AND created_at >= now() - interval '1 day'
                """, Integer.class, workspaceId);
        return value == null ? 0 : value;
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
