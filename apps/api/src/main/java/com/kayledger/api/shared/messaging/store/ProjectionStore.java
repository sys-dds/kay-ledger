package com.kayledger.api.shared.messaging.store;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProjectionStore {

    private final JdbcTemplate jdbcTemplate;

    public ProjectionStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void upsertPayment(UUID workspaceId, Map<String, Object> data) {
        jdbcTemplate.update("""
                INSERT INTO payment_projection (
                    workspace_id, payment_intent_id, booking_id, subscription_id, subscription_cycle_id,
                    latest_payment_status, gross_amount_minor, fee_amount_minor, net_amount_minor,
                    provider_profile_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (workspace_id, payment_intent_id) DO UPDATE
                SET booking_id = EXCLUDED.booking_id,
                    subscription_id = EXCLUDED.subscription_id,
                    subscription_cycle_id = EXCLUDED.subscription_cycle_id,
                    latest_payment_status = EXCLUDED.latest_payment_status,
                    gross_amount_minor = EXCLUDED.gross_amount_minor,
                    fee_amount_minor = EXCLUDED.fee_amount_minor,
                    net_amount_minor = EXCLUDED.net_amount_minor,
                    provider_profile_id = EXCLUDED.provider_profile_id,
                    updated_at = now()
                """,
                workspaceId,
                uuid(data.get("paymentIntentId")),
                nullableUuid(data.get("bookingId")),
                nullableUuid(data.get("subscriptionId")),
                nullableUuid(data.get("subscriptionCycleId")),
                string(data.get("status")),
                number(data.get("grossAmountMinor")),
                number(data.get("feeAmountMinor")),
                number(data.get("netAmountMinor")),
                uuid(data.get("providerProfileId")));
    }

    public void upsertSubscription(UUID workspaceId, UUID subscriptionId, Map<String, Object> data) {
        jdbcTemplate.update("""
                INSERT INTO subscription_projection (
                    workspace_id, subscription_id, latest_status, latest_entitlement_status,
                    current_cycle_number, current_plan_id, next_renewal_boundary, last_renewal_outcome, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (workspace_id, subscription_id) DO UPDATE
                SET latest_status = COALESCE(EXCLUDED.latest_status, subscription_projection.latest_status),
                    latest_entitlement_status = COALESCE(EXCLUDED.latest_entitlement_status, subscription_projection.latest_entitlement_status),
                    current_cycle_number = COALESCE(EXCLUDED.current_cycle_number, subscription_projection.current_cycle_number),
                    current_plan_id = COALESCE(EXCLUDED.current_plan_id, subscription_projection.current_plan_id),
                    next_renewal_boundary = COALESCE(EXCLUDED.next_renewal_boundary, subscription_projection.next_renewal_boundary),
                    last_renewal_outcome = COALESCE(EXCLUDED.last_renewal_outcome, subscription_projection.last_renewal_outcome),
                    updated_at = now()
                """,
                workspaceId,
                subscriptionId,
                (String) data.getOrDefault("status", data.get("latestStatus")),
                (String) data.get("entitlementStatus"),
                nullableInteger(data.get("cycleNumber")),
                nullableUuid(data.get("currentPlanId")),
                nullableInstant(data.get("nextRenewalBoundary")),
                (String) data.get("lastRenewalOutcome"));
    }

    public List<Map<String, Object>> listPayments(UUID workspaceId) {
        return jdbcTemplate.queryForList("""
                SELECT *
                FROM payment_projection
                WHERE workspace_id = ?
                ORDER BY updated_at DESC
                """, workspaceId);
    }

    public List<Map<String, Object>> listSubscriptions(UUID workspaceId) {
        return jdbcTemplate.queryForList("""
                SELECT *
                FROM subscription_projection
                WHERE workspace_id = ?
                ORDER BY updated_at DESC
                """, workspaceId);
    }

    private static UUID uuid(Object value) {
        UUID parsed = nullableUuid(value);
        if (parsed == null) {
            throw new IllegalArgumentException("Required UUID is missing.");
        }
        return parsed;
    }

    private static UUID nullableUuid(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof UUID uuid ? uuid : UUID.fromString(value.toString());
    }

    private static long number(Object value) {
        return value instanceof Number number ? number.longValue() : Long.parseLong(value.toString());
    }

    private static Integer nullableInteger(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof Number number ? number.intValue() : Integer.parseInt(value.toString());
    }

    private static Timestamp nullableInstant(Object value) {
        if (value == null) {
            return null;
        }
        return Timestamp.from(Instant.parse(value.toString()));
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }
}
