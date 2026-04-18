package com.kayledger.api.shared.messaging.store;

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

import com.kayledger.api.shared.messaging.model.OutboxEvent;

@Repository
public class OutboxStore {

    private static final RowMapper<OutboxEvent> MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("aggregate_type"),
            rs.getObject("aggregate_id", UUID.class),
            rs.getString("event_type"),
            rs.getString("payload_json"),
            rs.getString("dedupe_key"),
            rs.getString("status"),
            instant(rs, "available_at"),
            nullableInstant(rs, "published_at"),
            rs.getInt("retry_count"),
            rs.getString("last_error"),
            nullableInstant(rs, "parked_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public OutboxStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public OutboxEvent append(UUID workspaceId, String aggregateType, UUID aggregateId, String eventType, String payloadJson, String dedupeKey) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO outbox_events (
                    workspace_id, aggregate_type, aggregate_id, event_type, payload_json, dedupe_key
                )
                VALUES (?, ?, ?, ?, ?::jsonb, ?)
                ON CONFLICT (dedupe_key) DO UPDATE
                SET payload_json = outbox_events.payload_json
                RETURNING *
                """, MAPPER, workspaceId, aggregateType, aggregateId, eventType, payloadJson, dedupeKey);
    }

    public List<OutboxEvent> claimDue(int batchSize) {
        return jdbcTemplate.query("""
                WITH due AS (
                    SELECT id
                    FROM outbox_events
                    WHERE (
                        (status = 'PENDING' AND available_at <= now())
                        OR (status = 'CLAIMED' AND updated_at <= now() - interval '5 minutes')
                      )
                    ORDER BY available_at, created_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events o
                SET status = 'CLAIMED'
                FROM due
                WHERE o.id = due.id
                RETURNING o.*
                """, MAPPER, batchSize);
    }

    public List<OutboxEvent> claimDue(UUID workspaceId, int batchSize) {
        return jdbcTemplate.query("""
                WITH due AS (
                    SELECT id
                    FROM outbox_events
                    WHERE workspace_id = ?
                      AND (
                        (status = 'PENDING' AND available_at <= now())
                        OR (status = 'CLAIMED' AND updated_at <= now() - interval '5 minutes')
                      )
                    ORDER BY available_at, created_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events o
                SET status = 'CLAIMED'
                FROM due
                WHERE o.id = due.id
                RETURNING o.*
                """, MAPPER, workspaceId, batchSize);
    }

    public OutboxEvent markPublished(UUID eventId) {
        return jdbcTemplate.queryForObject("""
                UPDATE outbox_events
                SET status = 'PUBLISHED',
                    published_at = now(),
                    last_error = NULL
                WHERE id = ?
                RETURNING *
                """, MAPPER, eventId);
    }

    public OutboxEvent markFailure(UUID eventId, String error, int maxAttempts, long backoffSeconds) {
        return jdbcTemplate.queryForObject("""
                UPDATE outbox_events
                SET retry_count = retry_count + 1,
                    last_error = ?,
                    status = CASE WHEN retry_count + 1 >= ? THEN 'PARKED' ELSE 'PENDING' END,
                    parked_at = CASE WHEN retry_count + 1 >= ? THEN now() ELSE parked_at END,
                    available_at = CASE WHEN retry_count + 1 >= ? THEN available_at ELSE now() + (? * interval '1 second') END
                WHERE id = ?
                RETURNING *
                """, MAPPER, truncate(error), maxAttempts, maxAttempts, maxAttempts, backoffSeconds, eventId);
    }

    public Optional<OutboxEvent> find(UUID eventId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM outbox_events
                WHERE id = ?
                """, MAPPER, eventId).stream().findFirst();
    }

    public List<OutboxEvent> listParked(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM outbox_events
                WHERE (?::uuid IS NULL OR workspace_id = ?)
                  AND status = 'PARKED'
                ORDER BY parked_at, created_at
                """, MAPPER, workspaceId, workspaceId);
    }

    public List<OutboxEvent> listRecent(UUID workspaceId, int limit) {
        return jdbcTemplate.query("""
                SELECT *
                FROM outbox_events
                WHERE (?::uuid IS NULL OR workspace_id = ?)
                ORDER BY created_at DESC
                LIMIT ?
                """, MAPPER, workspaceId, workspaceId, limit);
    }

    public OutboxEvent replay(UUID workspaceId, UUID eventId) {
        return jdbcTemplate.queryForObject("""
                UPDATE outbox_events
                SET status = 'PENDING',
                    available_at = now(),
                    parked_at = NULL,
                    last_error = NULL
                WHERE id = ?
                  AND workspace_id = ?
                  AND status = 'PARKED'
                RETURNING *
                """, MAPPER, eventId, workspaceId);
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 1000 ? error : error.substring(0, 1000);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
