package com.kayledger.api.shared.messaging.store;

import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class InboxStore {

    private final JdbcTemplate jdbcTemplate;

    public InboxStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean beginProcessing(UUID workspaceId, String topic, int partitionId, String messageKey, UUID eventId, String dedupeKey, String consumerName, String payloadJson) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO inbox_messages (
                        workspace_id, topic, partition_id, message_key, event_id, dedupe_key, consumer_name, outcome, payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'PROCESSING', ?::jsonb)
                    """, workspaceId, topic, partitionId, messageKey, eventId, dedupeKey, consumerName, payloadJson);
            return true;
        } catch (DuplicateKeyException exception) {
            int claimed = jdbcTemplate.update("""
                    UPDATE inbox_messages
                    SET outcome = 'PROCESSING',
                        last_error = NULL,
                        payload_json = COALESCE(payload_json, ?::jsonb)
                    WHERE consumer_name = ?
                      AND dedupe_key = ?
                      AND outcome = 'FAILED'
                      AND available_at <= now()
                    """, payloadJson, consumerName, dedupeKey);
            return claimed == 1;
        }
    }

    public void recordSuccess(String dedupeKey, String consumerName) {
        jdbcTemplate.update("""
                UPDATE inbox_messages
                SET outcome = 'PROCESSED',
                    processed_at = now(),
                    last_error = NULL
                WHERE consumer_name = ?
                  AND dedupe_key = ?
                  AND outcome = 'PROCESSING'
                """, consumerName, dedupeKey);
    }

    public boolean recordFailure(String dedupeKey, String consumerName, String error, int maxAttempts, long backoffSeconds) {
        Integer parked = jdbcTemplate.queryForObject("""
                UPDATE inbox_messages
                SET retry_count = retry_count + 1,
                    last_error = ?,
                    outcome = CASE WHEN retry_count + 1 >= ? THEN 'PARKED' ELSE 'FAILED' END,
                    parked_at = CASE WHEN retry_count + 1 >= ? THEN now() ELSE parked_at END,
                    available_at = CASE WHEN retry_count + 1 >= ? THEN available_at ELSE now() + (? * interval '1 second') END
                WHERE consumer_name = ?
                  AND dedupe_key = ?
                  AND outcome = 'PROCESSING'
                RETURNING CASE WHEN outcome = 'PARKED' THEN 1 ELSE 0 END
                """, Integer.class, truncate(error), maxAttempts, maxAttempts, maxAttempts, backoffSeconds, consumerName, dedupeKey);
        return parked != null && parked == 1;
    }

    public int replayParked(UUID workspaceId, String consumerName, String dedupeKey) {
        return jdbcTemplate.update("""
                UPDATE inbox_messages
                SET outcome = 'FAILED',
                    available_at = now(),
                    parked_at = NULL,
                    last_error = NULL
                WHERE workspace_id = ?
                  AND consumer_name = ?
                  AND dedupe_key = ?
                  AND outcome = 'PARKED'
                """, workspaceId, consumerName, dedupeKey);
    }

    public Optional<ParkedMessage> findParked(UUID workspaceId, String consumerName, String dedupeKey) {
        return jdbcTemplate.query("""
                SELECT workspace_id, topic, partition_id, message_key, event_id, dedupe_key, payload_json::text AS payload_json
                FROM inbox_messages
                WHERE workspace_id = ?
                  AND consumer_name = ?
                  AND dedupe_key = ?
                  AND outcome = 'PARKED'
                """, (rs, rowNum) -> new ParkedMessage(
                rs.getObject("workspace_id", UUID.class),
                rs.getString("topic"),
                rs.getInt("partition_id"),
                rs.getString("message_key"),
                rs.getObject("event_id", UUID.class),
                rs.getString("dedupe_key"),
                rs.getString("payload_json")), workspaceId, consumerName, dedupeKey).stream().findFirst();
    }

    public List<Map<String, Object>> listParked(UUID workspaceId) {
        return jdbcTemplate.queryForList("""
                SELECT id, workspace_id, topic, message_key, event_id, dedupe_key, consumer_name,
                       retry_count, last_error, parked_at, updated_at
                FROM inbox_messages
                WHERE workspace_id = ?
                  AND outcome = 'PARKED'
                ORDER BY parked_at, id
                """, workspaceId);
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 1000 ? error : error.substring(0, 1000);
    }

    public record ParkedMessage(UUID workspaceId, String topic, int partitionId, String messageKey, UUID eventId, String dedupeKey, String payloadJson) {
    }
}
