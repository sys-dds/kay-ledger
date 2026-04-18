package com.kayledger.api.shared.messaging.store;

import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class InboxStore {

    private final JdbcTemplate jdbcTemplate;

    public InboxStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean recordSuccess(UUID workspaceId, String topic, int partitionId, String messageKey, UUID eventId, String dedupeKey, String consumerName) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO inbox_messages (
                        workspace_id, topic, partition_id, message_key, event_id, dedupe_key, consumer_name, outcome
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'PROCESSED')
                    """, workspaceId, topic, partitionId, messageKey, eventId, dedupeKey, consumerName);
            return true;
        } catch (DuplicateKeyException exception) {
            return false;
        }
    }
}
