package com.kayledger.api.provider.store;

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

import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.model.ProviderConfig;

@Repository
public class ProviderStore {

    private static final RowMapper<ProviderConfig> CONFIG_MAPPER = (rs, rowNum) -> new ProviderConfig(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("provider_key"),
            rs.getString("display_name"),
            rs.getString("signing_secret"),
            rs.getString("callback_token"),
            rs.getString("status"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ProviderCallback> CALLBACK_MAPPER = (rs, rowNum) -> new ProviderCallback(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_config_id", UUID.class),
            rs.getString("provider_key"),
            rs.getString("provider_event_id"),
            (Long) rs.getObject("provider_sequence"),
            rs.getString("callback_type"),
            rs.getString("business_reference_type"),
            rs.getObject("business_reference_id", UUID.class),
            rs.getString("payload_json"),
            rs.getString("signature_header"),
            rs.getBoolean("signature_verified"),
            rs.getString("dedupe_key"),
            rs.getString("processing_status"),
            rs.getString("processing_error"),
            nullableInstant(rs, "applied_at"),
            instant(rs, "received_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public ProviderStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ProviderConfig createConfig(UUID workspaceId, String providerKey, String displayName, String signingSecret, String callbackToken) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_configs (workspace_id, provider_key, display_name, signing_secret, callback_token)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, provider_key) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    signing_secret = EXCLUDED.signing_secret,
                    callback_token = EXCLUDED.callback_token,
                    status = 'ACTIVE'
                RETURNING *
                """, CONFIG_MAPPER, workspaceId, providerKey, displayName, signingSecret, callbackToken);
    }

    public Optional<ProviderConfig> findConfig(UUID workspaceId, String providerKey) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_configs
                WHERE workspace_id = ?
                  AND provider_key = ?
                  AND status = 'ACTIVE'
                """, CONFIG_MAPPER, workspaceId, providerKey).stream().findFirst();
    }

    public Optional<ProviderConfig> findConfigByCallbackToken(String callbackToken) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_configs
                WHERE callback_token = ?
                  AND status = 'ACTIVE'
                """, CONFIG_MAPPER, callbackToken).stream().findFirst();
    }

    public Optional<ProviderCallback> findCallbackByDedupe(UUID workspaceId, String providerKey, String dedupeKey) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_callbacks
                WHERE workspace_id = ?
                  AND provider_key = ?
                  AND dedupe_key = ?
                """, CALLBACK_MAPPER, workspaceId, providerKey, dedupeKey).stream().findFirst();
    }

    public Optional<ProviderCallback> findCallback(UUID workspaceId, UUID callbackId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_callbacks
                WHERE workspace_id = ?
                  AND id = ?
                """, CALLBACK_MAPPER, workspaceId, callbackId).stream().findFirst();
    }

    public List<ProviderCallback> listCallbacks(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_callbacks
                WHERE workspace_id = ?
                ORDER BY received_at, id
                """, CALLBACK_MAPPER, workspaceId);
    }

    public Optional<ProviderCallback> insertCallback(
            UUID workspaceId,
            UUID providerConfigId,
            String providerKey,
            String providerEventId,
            Long providerSequence,
            String callbackType,
            String businessReferenceType,
            UUID businessReferenceId,
            String payloadJson,
            String signatureHeader,
            boolean signatureVerified,
            String dedupeKey) {
        return jdbcTemplate.query("""
                INSERT INTO provider_callbacks (
                    workspace_id, provider_config_id, provider_key, provider_event_id, provider_sequence,
                    callback_type, business_reference_type, business_reference_id, payload_json,
                    signature_header, signature_verified, dedupe_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                ON CONFLICT (workspace_id, provider_key, dedupe_key) DO NOTHING
                RETURNING *
                """, CALLBACK_MAPPER, workspaceId, providerConfigId, providerKey, providerEventId, providerSequence,
                callbackType, businessReferenceType, businessReferenceId, payloadJson, signatureHeader, signatureVerified, dedupeKey).stream().findFirst();
    }

    public Long latestAppliedSequence(UUID workspaceId, String referenceType, UUID referenceId) {
        return jdbcTemplate.queryForObject("""
                SELECT MAX(provider_sequence)
                FROM provider_callbacks
                WHERE workspace_id = ?
                  AND business_reference_type = ?
                  AND business_reference_id = ?
                  AND processing_status = 'APPLIED'
                """, Long.class, workspaceId, referenceType, referenceId);
    }

    public ProviderCallback markApplied(UUID workspaceId, UUID callbackId) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_callbacks
                SET processing_status = 'APPLIED',
                    applied_at = now(),
                    processing_error = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, CALLBACK_MAPPER, workspaceId, callbackId);
    }

    public ProviderCallback markIgnoredOutOfOrder(UUID workspaceId, UUID callbackId) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_callbacks
                SET processing_status = 'IGNORED_OUT_OF_ORDER'
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, CALLBACK_MAPPER, workspaceId, callbackId);
    }

    public ProviderCallback markFailed(UUID workspaceId, UUID callbackId, String error) {
        return jdbcTemplate.queryForObject("""
                UPDATE provider_callbacks
                SET processing_status = 'FAILED',
                    processing_error = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, CALLBACK_MAPPER, truncate(error), workspaceId, callbackId);
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
