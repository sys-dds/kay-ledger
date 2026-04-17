package com.kayledger.api.subscription.store;

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

import com.kayledger.api.subscription.model.EntitlementGrant;
import com.kayledger.api.subscription.model.SubscriptionCycle;
import com.kayledger.api.subscription.model.SubscriptionPlan;
import com.kayledger.api.subscription.model.SubscriptionPlanChange;
import com.kayledger.api.subscription.model.SubscriptionRecord;

@Repository
public class SubscriptionStore {

    private static final RowMapper<SubscriptionPlan> PLAN_MAPPER = (rs, rowNum) -> new SubscriptionPlan(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("plan_code"),
            rs.getString("display_name"),
            rs.getString("billing_interval"),
            rs.getString("currency_code"),
            rs.getLong("amount_minor"),
            rs.getString("status"),
            rs.getInt("version"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<SubscriptionRecord> SUBSCRIPTION_MAPPER = (rs, rowNum) -> new SubscriptionRecord(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("customer_profile_id", UUID.class),
            rs.getObject("current_plan_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("status"),
            instant(rs, "start_at"),
            instant(rs, "current_period_start_at"),
            instant(rs, "current_period_end_at"),
            nullableInstant(rs, "grace_expires_at"),
            nullableInstant(rs, "cancelled_at"),
            nullableInstant(rs, "cancellation_effective_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<SubscriptionCycle> CYCLE_MAPPER = (rs, rowNum) -> new SubscriptionCycle(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("subscription_id", UUID.class),
            rs.getInt("cycle_number"),
            rs.getObject("plan_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getObject("customer_profile_id", UUID.class),
            instant(rs, "cycle_start_at"),
            instant(rs, "cycle_end_at"),
            rs.getString("status"),
            rs.getObject("payment_intent_id", UUID.class),
            rs.getString("external_reference"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<SubscriptionPlanChange> PLAN_CHANGE_MAPPER = (rs, rowNum) -> new SubscriptionPlanChange(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("subscription_id", UUID.class),
            rs.getObject("target_plan_id", UUID.class),
            (Integer) rs.getObject("effective_cycle_number"),
            nullableInstant(rs, "effective_at"),
            rs.getString("status"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<EntitlementGrant> ENTITLEMENT_MAPPER = (rs, rowNum) -> new EntitlementGrant(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("subscription_id", UUID.class),
            rs.getObject("customer_profile_id", UUID.class),
            rs.getString("status"),
            rs.getString("entitlement_code"),
            instant(rs, "starts_at"),
            nullableInstant(rs, "ends_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public SubscriptionStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public SubscriptionPlan createPlan(UUID workspaceId, UUID providerProfileId, String planCode, String displayName, String billingInterval, String currencyCode, long amountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO subscription_plans (
                    workspace_id, provider_profile_id, plan_code, display_name, billing_interval, currency_code, amount_minor
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, PLAN_MAPPER, workspaceId, providerProfileId, planCode, displayName, billingInterval, currencyCode, amountMinor);
    }

    public Optional<SubscriptionPlan> findPlan(UUID workspaceId, UUID planId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscription_plans
                WHERE workspace_id = ?
                  AND id = ?
                """, PLAN_MAPPER, workspaceId, planId).stream().findFirst();
    }

    public List<SubscriptionPlan> listPlans(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscription_plans
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, PLAN_MAPPER, workspaceId);
    }

    public SubscriptionRecord createSubscription(UUID workspaceId, UUID customerProfileId, SubscriptionPlan plan, Instant startAt, Instant periodEndAt) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO subscriptions (
                    workspace_id, customer_profile_id, current_plan_id, provider_profile_id, status,
                    start_at, current_period_start_at, current_period_end_at
                )
                VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
                RETURNING *
                """, SUBSCRIPTION_MAPPER, workspaceId, customerProfileId, plan.id(), plan.providerProfileId(), timestamp(startAt), timestamp(startAt), timestamp(periodEndAt));
    }

    public Optional<SubscriptionRecord> findSubscription(UUID workspaceId, UUID subscriptionId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscriptions
                WHERE workspace_id = ?
                  AND id = ?
                """, SUBSCRIPTION_MAPPER, workspaceId, subscriptionId).stream().findFirst();
    }

    public List<SubscriptionRecord> listSubscriptions(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscriptions
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, SUBSCRIPTION_MAPPER, workspaceId);
    }

    public List<SubscriptionRecord> dueSubscriptions(UUID workspaceId, Instant now) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscriptions
                WHERE workspace_id = ?
                  AND status IN ('ACTIVE', 'GRACE')
                  AND current_period_end_at <= ?
                ORDER BY current_period_end_at, id
                """, SUBSCRIPTION_MAPPER, workspaceId, timestamp(now));
    }

    public List<SubscriptionRecord> graceExpiredSubscriptions(UUID workspaceId, Instant now) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscriptions
                WHERE workspace_id = ?
                  AND status = 'GRACE'
                  AND grace_expires_at <= ?
                ORDER BY grace_expires_at, id
                """, SUBSCRIPTION_MAPPER, workspaceId, timestamp(now));
    }

    public SubscriptionRecord moveToGrace(UUID workspaceId, UUID subscriptionId, Instant graceExpiresAt) {
        return jdbcTemplate.queryForObject("""
                UPDATE subscriptions
                SET status = 'GRACE',
                    grace_expires_at = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, SUBSCRIPTION_MAPPER, timestamp(graceExpiresAt), workspaceId, subscriptionId);
    }

    public SubscriptionRecord moveToSuspended(UUID workspaceId, UUID subscriptionId) {
        return jdbcTemplate.queryForObject("""
                UPDATE subscriptions
                SET status = 'SUSPENDED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'GRACE'
                RETURNING *
                """, SUBSCRIPTION_MAPPER, workspaceId, subscriptionId);
    }

    public SubscriptionRecord cancel(UUID workspaceId, UUID subscriptionId, Instant cancelledAt) {
        return jdbcTemplate.queryForObject("""
                UPDATE subscriptions
                SET status = 'CANCELLED',
                    cancelled_at = ?,
                    cancellation_effective_at = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status <> 'CANCELLED'
                RETURNING *
                """, SUBSCRIPTION_MAPPER, timestamp(cancelledAt), timestamp(cancelledAt), workspaceId, subscriptionId);
    }

    public SubscriptionRecord advancePeriod(UUID workspaceId, UUID subscriptionId, UUID planId, UUID providerProfileId, Instant periodStart, Instant periodEnd) {
        return jdbcTemplate.queryForObject("""
                UPDATE subscriptions
                SET status = 'ACTIVE',
                    current_plan_id = ?,
                    provider_profile_id = ?,
                    current_period_start_at = ?,
                    current_period_end_at = ?,
                    grace_expires_at = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, SUBSCRIPTION_MAPPER, planId, providerProfileId, timestamp(periodStart), timestamp(periodEnd), workspaceId, subscriptionId);
    }

    public int nextCycleNumber(UUID workspaceId, UUID subscriptionId) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT COALESCE(MAX(cycle_number), 0) + 1
                FROM subscription_cycles
                WHERE workspace_id = ?
                  AND subscription_id = ?
                """, Integer.class, workspaceId, subscriptionId);
        return value == null ? 1 : value;
    }

    public SubscriptionCycle createCycle(UUID workspaceId, UUID subscriptionId, int cycleNumber, SubscriptionPlan plan, UUID customerProfileId, Instant startAt, Instant endAt, String status, String externalReference) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO subscription_cycles (
                    workspace_id, subscription_id, cycle_number, plan_id, provider_profile_id, customer_profile_id,
                    cycle_start_at, cycle_end_at, status, external_reference
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, subscription_id, cycle_number) DO UPDATE
                SET external_reference = COALESCE(subscription_cycles.external_reference, EXCLUDED.external_reference)
                RETURNING *
                """, CYCLE_MAPPER, workspaceId, subscriptionId, cycleNumber, plan.id(), plan.providerProfileId(), customerProfileId, timestamp(startAt), timestamp(endAt), status, externalReference);
    }

    public List<SubscriptionCycle> listCycles(UUID workspaceId, UUID subscriptionId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscription_cycles
                WHERE workspace_id = ?
                  AND subscription_id = ?
                ORDER BY cycle_number
                """, CYCLE_MAPPER, workspaceId, subscriptionId);
    }

    public SubscriptionCycle attachPaymentIntent(UUID workspaceId, UUID cycleId, UUID paymentIntentId) {
        return jdbcTemplate.queryForObject("""
                UPDATE subscription_cycles
                SET payment_intent_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND payment_intent_id IS NULL
                RETURNING *
                """, CYCLE_MAPPER, paymentIntentId, workspaceId, cycleId);
    }

    public SubscriptionPlanChange schedulePlanChange(UUID workspaceId, UUID subscriptionId, UUID targetPlanId, int effectiveCycleNumber) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO subscription_plan_changes (
                    workspace_id, subscription_id, target_plan_id, effective_cycle_number
                )
                VALUES (?, ?, ?, ?)
                RETURNING *
                """, PLAN_CHANGE_MAPPER, workspaceId, subscriptionId, targetPlanId, effectiveCycleNumber);
    }

    public Optional<SubscriptionPlanChange> pendingPlanChange(UUID workspaceId, UUID subscriptionId, int cycleNumber, Instant effectiveAt) {
        return jdbcTemplate.query("""
                SELECT *
                FROM subscription_plan_changes
                WHERE workspace_id = ?
                  AND subscription_id = ?
                  AND status = 'SCHEDULED'
                  AND (effective_cycle_number <= ? OR effective_at <= ?)
                ORDER BY created_at, id
                LIMIT 1
                """, PLAN_CHANGE_MAPPER, workspaceId, subscriptionId, cycleNumber, timestamp(effectiveAt)).stream().findFirst();
    }

    public void markPlanChangeApplied(UUID workspaceId, UUID planChangeId) {
        jdbcTemplate.update("""
                UPDATE subscription_plan_changes
                SET status = 'APPLIED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'SCHEDULED'
                """, workspaceId, planChangeId);
    }

    public EntitlementGrant upsertEntitlement(UUID workspaceId, UUID subscriptionId, UUID customerProfileId, String status, Instant startsAt, Instant endsAt) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO entitlement_grants (
                    workspace_id, subscription_id, customer_profile_id, status, entitlement_code, starts_at, ends_at
                )
                VALUES (?, ?, ?, ?, 'SUBSCRIPTION_ACCESS', ?, ?)
                ON CONFLICT (workspace_id, subscription_id, entitlement_code) DO UPDATE
                SET status = EXCLUDED.status,
                    ends_at = EXCLUDED.ends_at
                RETURNING *
                """, ENTITLEMENT_MAPPER, workspaceId, subscriptionId, customerProfileId, status, timestamp(startsAt), timestampNullable(endsAt));
    }

    public List<EntitlementGrant> listEntitlements(UUID workspaceId, UUID subscriptionId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM entitlement_grants
                WHERE workspace_id = ?
                  AND subscription_id = ?
                ORDER BY entitlement_code
                """, ENTITLEMENT_MAPPER, workspaceId, subscriptionId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        var timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }

    private static Timestamp timestampNullable(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
