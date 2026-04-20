package com.kayledger.api.merchantevents.store;

import java.sql.Array;
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

import com.kayledger.api.merchantevents.model.MerchantFinanceEndpoint;
import com.kayledger.api.merchantevents.model.MerchantFinanceEvent;
import com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery;

@Repository
public class MerchantFinanceEventStore {

    private static final RowMapper<MerchantFinanceEndpoint> ENDPOINT_MAPPER = (rs, rowNum) -> new MerchantFinanceEndpoint(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("endpoint_url"),
            rs.getString("signing_secret_ref"),
            rs.getString("status"),
            stringArray(rs, "event_types"),
            rs.getObject("created_by_actor_id", UUID.class),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<MerchantFinanceEvent> EVENT_MAPPER = (rs, rowNum) -> new MerchantFinanceEvent(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getObject("accounting_period_id", UUID.class),
            rs.getString("event_type"),
            rs.getString("source_reference_type"),
            rs.getObject("source_reference_id", UUID.class),
            rs.getString("payload_json"),
            rs.getString("event_key"),
            instant(rs, "occurred_at"),
            instant(rs, "created_at"));

    private static final RowMapper<MerchantFinanceEventDelivery> DELIVERY_MAPPER = (rs, rowNum) -> new MerchantFinanceEventDelivery(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("merchant_finance_event_id", UUID.class),
            rs.getObject("endpoint_id", UUID.class),
            rs.getString("delivery_status"),
            rs.getInt("attempt_count"),
            nullableInstant(rs, "first_attempt_at"),
            nullableInstant(rs, "last_attempt_at"),
            nullableInstant(rs, "next_attempt_at"),
            nullableInteger(rs, "response_status"),
            rs.getString("response_body"),
            rs.getString("final_failure_reason"),
            rs.getString("parked_reason"),
            rs.getString("signature_algorithm"),
            rs.getString("signature_value"),
            rs.getString("dedupe_key"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public MerchantFinanceEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public MerchantFinanceEndpoint createEndpoint(UUID workspaceId, UUID providerProfileId, String endpointUrl, String signingSecretRef, String[] eventTypes, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO merchant_finance_event_endpoints (
                    workspace_id, provider_profile_id, endpoint_url, signing_secret_ref, event_types, created_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING *
                """, ENDPOINT_MAPPER, workspaceId, providerProfileId, endpointUrl, signingSecretRef, eventTypes, actorId);
    }

    public List<MerchantFinanceEndpoint> listEndpoints(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM merchant_finance_event_endpoints
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, ENDPOINT_MAPPER, workspaceId);
    }

    public List<MerchantFinanceEndpoint> matchingEndpoints(UUID workspaceId, UUID providerProfileId, String eventType) {
        return jdbcTemplate.query("""
                SELECT *
                FROM merchant_finance_event_endpoints
                WHERE workspace_id = ?
                  AND status = 'ACTIVE'
                  AND (provider_profile_id IS NULL OR provider_profile_id = ?)
                  AND (cardinality(event_types) = 0 OR ? = ANY(event_types))
                ORDER BY created_at, id
                """, ENDPOINT_MAPPER, workspaceId, providerProfileId, eventType);
    }

    public MerchantFinanceEvent upsertEvent(UUID workspaceId, UUID providerProfileId, String currencyCode, UUID accountingPeriodId, String eventType, String sourceType, UUID sourceId, String payloadJson, String eventKey, Instant occurredAt) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO merchant_finance_events (
                    workspace_id, provider_profile_id, currency_code, accounting_period_id, event_type,
                    source_reference_type, source_reference_id, payload_json, event_key, occurred_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (workspace_id, event_key) DO UPDATE
                SET event_key = merchant_finance_events.event_key
                RETURNING *, payload_json::text
                """, EVENT_MAPPER, workspaceId, providerProfileId, currencyCode, accountingPeriodId, eventType, sourceType, sourceId, payloadJson, eventKey, Timestamp.from(occurredAt));
    }

    public MerchantFinanceEventDelivery upsertDelivery(UUID workspaceId, UUID eventId, UUID endpointId, String dedupeKey, String signatureValue) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO merchant_finance_event_deliveries (
                    workspace_id, merchant_finance_event_id, endpoint_id, dedupe_key, signature_value
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, dedupe_key) DO UPDATE
                SET dedupe_key = merchant_finance_event_deliveries.dedupe_key
                RETURNING *
                """, DELIVERY_MAPPER, workspaceId, eventId, endpointId, dedupeKey, signatureValue);
    }

    public List<MerchantFinanceEventDelivery> listDeliveries(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM merchant_finance_event_deliveries
                WHERE workspace_id = ?
                ORDER BY updated_at DESC, id
                """, DELIVERY_MAPPER, workspaceId);
    }

    public MerchantFinanceEventDelivery recordAttempt(UUID workspaceId, UUID deliveryId, String status, Integer responseStatus, String responseBody) {
        return jdbcTemplate.queryForObject("""
                UPDATE merchant_finance_event_deliveries
                SET delivery_status = ?,
                    attempt_count = attempt_count + 1,
                    first_attempt_at = COALESCE(first_attempt_at, now()),
                    last_attempt_at = now(),
                    next_attempt_at = CASE WHEN ? = 'FAILED' THEN now() + interval '5 minutes' ELSE NULL END,
                    response_status = ?,
                    response_body = ?,
                    final_failure_reason = CASE WHEN ? = 'FAILED' THEN ? ELSE NULL END,
                    parked_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, DELIVERY_MAPPER, status, status, responseStatus, responseBody, status, responseBody, workspaceId, deliveryId);
    }

    public List<DeliveryWork> claimDueDeliveries(int limit, int maxAttempts) {
        return jdbcTemplate.query("""
                SELECT d.*,
                       e.event_type,
                       e.source_reference_type,
                       e.source_reference_id,
                       e.payload_json::text AS payload_json,
                       e.event_key,
                       e.occurred_at,
                       endpoint.endpoint_url
                FROM merchant_finance_event_deliveries d
                JOIN merchant_finance_events e
                  ON e.workspace_id = d.workspace_id
                 AND e.id = d.merchant_finance_event_id
                JOIN merchant_finance_event_endpoints endpoint
                  ON endpoint.workspace_id = d.workspace_id
                 AND endpoint.id = d.endpoint_id
                 AND endpoint.status = 'ACTIVE'
                WHERE (
                    d.delivery_status = 'PENDING'
                    OR (d.delivery_status = 'FAILED' AND d.attempt_count < ?)
                  )
                  AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= now())
                ORDER BY d.created_at, d.id
                LIMIT ?
                FOR UPDATE OF d SKIP LOCKED
                """, DELIVERY_WORK_MAPPER, maxAttempts, limit);
    }

    public MerchantFinanceEventDelivery recordDeliverySuccess(UUID workspaceId, UUID deliveryId, int responseStatus, String responseBody) {
        return jdbcTemplate.queryForObject("""
                UPDATE merchant_finance_event_deliveries
                SET delivery_status = 'SUCCEEDED',
                    attempt_count = attempt_count + 1,
                    first_attempt_at = COALESCE(first_attempt_at, now()),
                    last_attempt_at = now(),
                    next_attempt_at = NULL,
                    response_status = ?,
                    response_body = ?,
                    final_failure_reason = NULL,
                    parked_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, DELIVERY_MAPPER, responseStatus, responseBody, workspaceId, deliveryId);
    }

    public MerchantFinanceEventDelivery recordDeliveryFailure(UUID workspaceId, UUID deliveryId, int maxAttempts, long backoffSeconds, Integer responseStatus, String responseBody, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE merchant_finance_event_deliveries
                SET delivery_status = CASE WHEN attempt_count + 1 >= ? THEN 'PARKED' ELSE 'FAILED' END,
                    attempt_count = attempt_count + 1,
                    first_attempt_at = COALESCE(first_attempt_at, now()),
                    last_attempt_at = now(),
                    next_attempt_at = CASE WHEN attempt_count + 1 >= ? THEN NULL ELSE now() + (? * interval '1 second') END,
                    response_status = ?,
                    response_body = ?,
                    final_failure_reason = ?,
                    parked_reason = CASE WHEN attempt_count + 1 >= ? THEN ? ELSE NULL END
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, DELIVERY_MAPPER, maxAttempts, maxAttempts, backoffSeconds, responseStatus, responseBody, failureReason, maxAttempts, failureReason, workspaceId, deliveryId);
    }

    public MerchantFinanceEventDelivery redriveDelivery(UUID workspaceId, UUID deliveryId) {
        return jdbcTemplate.queryForObject("""
                UPDATE merchant_finance_event_deliveries
                SET delivery_status = 'PENDING',
                    next_attempt_at = now(),
                    final_failure_reason = NULL,
                    parked_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND delivery_status IN ('FAILED', 'PARKED')
                RETURNING *
                """, DELIVERY_MAPPER, workspaceId, deliveryId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Integer nullableInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static String[] stringArray(ResultSet rs, String column) throws SQLException {
        Array array = rs.getArray(column);
        return array == null ? new String[0] : (String[]) array.getArray();
    }

    private static final RowMapper<DeliveryWork> DELIVERY_WORK_MAPPER = (rs, rowNum) -> new DeliveryWork(
            DELIVERY_MAPPER.mapRow(rs, rowNum),
            rs.getString("event_type"),
            rs.getString("source_reference_type"),
            rs.getObject("source_reference_id", UUID.class),
            rs.getString("payload_json"),
            rs.getString("event_key"),
            instant(rs, "occurred_at"),
            rs.getString("endpoint_url"));

    public record DeliveryWork(
            MerchantFinanceEventDelivery delivery,
            String eventType,
            String sourceReferenceType,
            UUID sourceReferenceId,
            String payloadJson,
            String eventKey,
            Instant occurredAt,
            String endpointUrl) {
    }
}
