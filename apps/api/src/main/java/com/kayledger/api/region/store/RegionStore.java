package com.kayledger.api.region.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.region.model.WorkspaceRegionFailoverEvent;
import com.kayledger.api.region.model.WorkspaceRegionOwnership;
import com.kayledger.api.region.application.RegionReplicationService.RegionReplicationEvent;
import com.kayledger.api.region.model.RegionInvestigationSnapshot;
import com.kayledger.api.region.model.RegionProviderSummarySnapshot;
import com.kayledger.api.region.model.RegionReplicationCheckpoint;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Repository
public class RegionStore {

    private static final RowMapper<WorkspaceRegionOwnership> OWNERSHIP_MAPPER = (rs, rowNum) -> mapOwnership(rs);
    private static final RowMapper<WorkspaceRegionFailoverEvent> FAILOVER_MAPPER = (rs, rowNum) -> new WorkspaceRegionFailoverEvent(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("from_region"),
            rs.getString("to_region"),
            rs.getLong("prior_epoch"),
            rs.getLong("new_epoch"),
            rs.getString("trigger_mode"),
            rs.getObject("requested_by_actor_id", UUID.class),
            rs.getTimestamp("created_at").toInstant());
    private static final RowMapper<RegionReplicationCheckpoint> CHECKPOINT_MAPPER = (rs, rowNum) -> new RegionReplicationCheckpoint(
            rs.getString("source_region"),
            rs.getString("target_region"),
            rs.getString("stream_name"),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("last_applied_event_id", UUID.class),
            rs.getLong("last_applied_sequence"),
            nullableInstant(rs, "last_applied_at"),
            rs.getLong("lag_millis"),
            rs.getTimestamp("updated_at").toInstant());
    private static final RowMapper<RegionInvestigationSnapshot> INVESTIGATION_SNAPSHOT_MAPPER = (rs, rowNum) -> new RegionInvestigationSnapshot(
            rs.getString("document_id"),
            rs.getString("document_type"),
            rs.getString("reference_type"),
            rs.getString("reference_id"),
            rs.getString("status"),
            rs.getString("payload_json"),
            rs.getString("source_region"),
            rs.getString("target_region"),
            rs.getObject("replication_event_id", UUID.class),
            rs.getTimestamp("replicated_at").toInstant());
    private static final RowMapper<RegionProviderSummarySnapshot> PROVIDER_SUMMARY_SNAPSHOT_MAPPER = (rs, rowNum) -> new RegionProviderSummarySnapshot(
            new ProviderFinancialSummary(
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("provider_profile_id", UUID.class),
                    rs.getString("currency_code"),
                    rs.getLong("settled_gross_amount_minor"),
                    rs.getLong("fee_amount_minor"),
                    rs.getLong("net_earnings_amount_minor"),
                    rs.getLong("current_payout_requested_amount_minor"),
                    rs.getLong("payout_succeeded_amount_minor"),
                    rs.getLong("refund_amount_minor"),
                    rs.getLong("active_dispute_exposure_amount_minor"),
                    rs.getLong("settled_subscription_net_revenue_amount_minor"),
                    rs.getTimestamp("source_refreshed_at").toInstant()),
            rs.getString("source_region"),
            rs.getString("target_region"),
            rs.getObject("replication_event_id", UUID.class),
            rs.getTimestamp("replicated_at").toInstant());

    private final JdbcTemplate jdbcTemplate;

    public RegionStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public WorkspaceRegionOwnership ensureOwnership(UUID workspaceId, String homeRegion) {
        jdbcTemplate.update("""
                INSERT INTO workspace_region_ownership (workspace_id, home_region)
                VALUES (?, ?)
                ON CONFLICT (workspace_id) DO NOTHING
                """, workspaceId, homeRegion);
        return findOwnership(workspaceId)
                .orElseThrow(() -> new IllegalStateException("Workspace ownership could not be resolved."));
    }

    public WorkspaceRegionOwnership upsertReplicatedOwnership(UUID workspaceId, String homeRegion, long ownershipEpoch) {
        jdbcTemplate.update("""
                INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch, status, transfer_state)
                VALUES (?, ?, ?, 'ACTIVE', 'TRANSFERRED')
                ON CONFLICT (workspace_id) DO UPDATE
                SET home_region = EXCLUDED.home_region,
                    ownership_epoch = EXCLUDED.ownership_epoch,
                    status = 'ACTIVE',
                    transfer_state = 'TRANSFERRED'
                WHERE workspace_region_ownership.ownership_epoch <= EXCLUDED.ownership_epoch
                """, workspaceId, homeRegion, ownershipEpoch);
        return findOwnership(workspaceId)
                .orElseThrow(() -> new IllegalStateException("Replicated workspace ownership could not be resolved."));
    }

    public Optional<WorkspaceRegionOwnership> findOwnership(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT workspace_id, home_region, ownership_epoch, status, transfer_state, updated_at, created_at
                FROM workspace_region_ownership
                WHERE workspace_id = ?
                """, OWNERSHIP_MAPPER, workspaceId).stream().findFirst();
    }

    public WorkspaceRegionOwnership lockOwnership(UUID workspaceId) {
        return jdbcTemplate.queryForObject("""
                SELECT workspace_id, home_region, ownership_epoch, status, transfer_state, updated_at, created_at
                FROM workspace_region_ownership
                WHERE workspace_id = ?
                FOR UPDATE
                """, OWNERSHIP_MAPPER, workspaceId);
    }

    public WorkspaceRegionOwnership transferOwnership(UUID workspaceId, String toRegion, long newEpoch) {
        return jdbcTemplate.queryForObject("""
                UPDATE workspace_region_ownership
                SET home_region = ?,
                    ownership_epoch = ?,
                    status = 'ACTIVE',
                    transfer_state = 'TRANSFERRED'
                WHERE workspace_id = ?
                RETURNING workspace_id, home_region, ownership_epoch, status, transfer_state, updated_at, created_at
                """, OWNERSHIP_MAPPER, toRegion, newEpoch, workspaceId);
    }

    public WorkspaceRegionFailoverEvent recordFailover(UUID workspaceId, String fromRegion, String toRegion, long priorEpoch, long newEpoch, String triggerMode, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO workspace_region_failover_events (
                    workspace_id, from_region, to_region, prior_epoch, new_epoch, trigger_mode, requested_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING id, workspace_id, from_region, to_region, prior_epoch, new_epoch, trigger_mode, requested_by_actor_id, created_at
                """, FAILOVER_MAPPER, workspaceId, fromRegion, toRegion, priorEpoch, newEpoch, triggerMode, actorId);
    }

    public void recordReplicatedFailover(UUID workspaceId, String fromRegion, String toRegion, long priorEpoch, long newEpoch, String triggerMode, UUID actorId) {
        jdbcTemplate.update("""
                INSERT INTO workspace_region_failover_events (
                    workspace_id, from_region, to_region, prior_epoch, new_epoch, trigger_mode, requested_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, from_region, to_region, prior_epoch, new_epoch, trigger_mode) DO NOTHING
                """, workspaceId, fromRegion, toRegion, priorEpoch, newEpoch, triggerMode, actorId);
    }

    public List<WorkspaceRegionFailoverEvent> listFailoverEvents(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, from_region, to_region, prior_epoch, new_epoch, trigger_mode, requested_by_actor_id, created_at
                FROM workspace_region_failover_events
                WHERE workspace_id = ?
                ORDER BY created_at DESC
                """, FAILOVER_MAPPER, workspaceId);
    }

    public void upsertInvestigationSnapshot(RegionReplicationEvent event, com.kayledger.api.investigation.model.InvestigationDocument document, String payloadJson, long lagMillis) {
        int applied = jdbcTemplate.update("""
                INSERT INTO region_investigation_read_snapshots (
                    workspace_id, source_region, target_region, replication_event_id,
                    document_id, document_type, reference_type, reference_id,
                    provider_profile_id, payment_intent_id, refund_id, payout_request_id, dispute_id, subscription_id,
                    provider_event_id, external_reference, business_reference_id, status, occurred_at,
                    payload_json, source_updated_at, replication_sequence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (target_region, workspace_id, document_id) DO UPDATE
                SET replication_event_id = EXCLUDED.replication_event_id,
                    replication_sequence = EXCLUDED.replication_sequence,
                    document_type = EXCLUDED.document_type,
                    reference_type = EXCLUDED.reference_type,
                    reference_id = EXCLUDED.reference_id,
                    provider_profile_id = EXCLUDED.provider_profile_id,
                    payment_intent_id = EXCLUDED.payment_intent_id,
                    refund_id = EXCLUDED.refund_id,
                    payout_request_id = EXCLUDED.payout_request_id,
                    dispute_id = EXCLUDED.dispute_id,
                    subscription_id = EXCLUDED.subscription_id,
                    provider_event_id = EXCLUDED.provider_event_id,
                    external_reference = EXCLUDED.external_reference,
                    business_reference_id = EXCLUDED.business_reference_id,
                    status = EXCLUDED.status,
                    occurred_at = EXCLUDED.occurred_at,
                    payload_json = EXCLUDED.payload_json,
                    source_updated_at = EXCLUDED.source_updated_at,
                    replicated_at = now()
                WHERE region_investigation_read_snapshots.source_updated_at <= EXCLUDED.source_updated_at
                  AND region_investigation_read_snapshots.replication_sequence <= EXCLUDED.replication_sequence
                """,
                event.workspaceId(),
                event.sourceRegion(),
                event.targetRegion(),
                event.eventId(),
                document.documentId(),
                document.documentType(),
                document.referenceType(),
                document.referenceId().toString(),
                string(document.providerProfileId()),
                string(document.paymentIntentId()),
                string(document.refundId()),
                string(document.payoutRequestId()),
                string(document.disputeId()),
                string(document.subscriptionId()),
                document.providerEventId(),
                document.externalReference(),
                string(document.businessReferenceId()),
                document.status(),
                timestamp(document.occurredAt()),
                payloadJson,
                timestamp(event.occurredAt()),
                event.sequence());
        if (applied > 0) {
            upsertReplicationCheckpoint(event, lagMillis);
        }
    }

    public void upsertProviderSummarySnapshot(RegionReplicationEvent event, ProviderFinancialSummary summary, long lagMillis) {
        int applied = jdbcTemplate.update("""
                INSERT INTO region_provider_summary_snapshots (
                    workspace_id, source_region, target_region, replication_event_id,
                    provider_profile_id, currency_code,
                    settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                    current_payout_requested_amount_minor, payout_succeeded_amount_minor, refund_amount_minor,
                    active_dispute_exposure_amount_minor, settled_subscription_net_revenue_amount_minor, source_refreshed_at, replication_sequence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (target_region, workspace_id, provider_profile_id, currency_code) DO UPDATE
                SET replication_event_id = EXCLUDED.replication_event_id,
                    replication_sequence = EXCLUDED.replication_sequence,
                    settled_gross_amount_minor = EXCLUDED.settled_gross_amount_minor,
                    fee_amount_minor = EXCLUDED.fee_amount_minor,
                    net_earnings_amount_minor = EXCLUDED.net_earnings_amount_minor,
                    current_payout_requested_amount_minor = EXCLUDED.current_payout_requested_amount_minor,
                    payout_succeeded_amount_minor = EXCLUDED.payout_succeeded_amount_minor,
                    refund_amount_minor = EXCLUDED.refund_amount_minor,
                    active_dispute_exposure_amount_minor = EXCLUDED.active_dispute_exposure_amount_minor,
                    settled_subscription_net_revenue_amount_minor = EXCLUDED.settled_subscription_net_revenue_amount_minor,
                    source_refreshed_at = EXCLUDED.source_refreshed_at,
                    replicated_at = now()
                WHERE region_provider_summary_snapshots.source_refreshed_at <= EXCLUDED.source_refreshed_at
                  AND region_provider_summary_snapshots.replication_sequence <= EXCLUDED.replication_sequence
                """,
                event.workspaceId(),
                event.sourceRegion(),
                event.targetRegion(),
                event.eventId(),
                summary.providerProfileId(),
                summary.currencyCode(),
                summary.settledGrossAmountMinor(),
                summary.feeAmountMinor(),
                summary.netEarningsAmountMinor(),
                summary.currentPayoutRequestedAmountMinor(),
                summary.payoutSucceededAmountMinor(),
                summary.refundAmountMinor(),
                summary.activeDisputeExposureAmountMinor(),
                summary.settledSubscriptionNetRevenueAmountMinor(),
                timestamp(summary.refreshedAt()),
                event.sequence());
        if (applied > 0) {
            upsertReplicationCheckpoint(event, lagMillis);
        }
    }

    public List<RegionInvestigationSnapshot> searchInvestigationSnapshots(
            String localRegion,
            UUID workspaceId,
            String paymentId,
            String refundId,
            String payoutId,
            String disputeId,
            String providerEventId,
            String externalReference,
            String businessReferenceId,
            String subscriptionId,
            String providerProfileId,
            String referenceId) {
        return jdbcTemplate.query("""
                SELECT document_id, document_type, reference_type, reference_id, status, payload_json::text,
                       source_region, target_region, replication_event_id, replicated_at
                FROM region_investigation_read_snapshots
                WHERE target_region = ?
                  AND workspace_id = ?
                  AND (?::text IS NULL OR payment_intent_id = ?::text)
                  AND (?::text IS NULL OR refund_id = ?::text)
                  AND (?::text IS NULL OR payout_request_id = ?::text)
                  AND (?::text IS NULL OR dispute_id = ?::text)
                  AND (?::text IS NULL OR provider_event_id = ?::text)
                  AND (?::text IS NULL OR external_reference = ?::text)
                  AND (?::text IS NULL OR business_reference_id = ?::text)
                  AND (?::text IS NULL OR subscription_id = ?::text)
                  AND (?::text IS NULL OR provider_profile_id = ?::text)
                  AND (?::text IS NULL OR reference_id = ?::text)
                ORDER BY occurred_at DESC NULLS LAST, replicated_at DESC
                LIMIT 50
                """, INVESTIGATION_SNAPSHOT_MAPPER,
                localRegion, workspaceId,
                blankToNull(paymentId), blankToNull(paymentId),
                blankToNull(refundId), blankToNull(refundId),
                blankToNull(payoutId), blankToNull(payoutId),
                blankToNull(disputeId), blankToNull(disputeId),
                blankToNull(providerEventId), blankToNull(providerEventId),
                blankToNull(externalReference), blankToNull(externalReference),
                blankToNull(businessReferenceId), blankToNull(businessReferenceId),
                blankToNull(subscriptionId), blankToNull(subscriptionId),
                blankToNull(providerProfileId), blankToNull(providerProfileId),
                blankToNull(referenceId), blankToNull(referenceId));
    }

    public List<RegionProviderSummarySnapshot> providerSummarySnapshots(String localRegion, UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM region_provider_summary_snapshots
                WHERE target_region = ?
                  AND workspace_id = ?
                ORDER BY provider_profile_id, currency_code
                """, PROVIDER_SUMMARY_SNAPSHOT_MAPPER, localRegion, workspaceId);
    }

    public List<RegionReplicationCheckpoint> replicationCheckpoints(String localRegion) {
        return jdbcTemplate.query("""
                SELECT source_region, target_region, stream_name, workspace_id, last_applied_event_id, last_applied_sequence, last_applied_at, lag_millis, updated_at
                FROM region_replication_checkpoints
                WHERE target_region = ?
                ORDER BY source_region, stream_name
                """, CHECKPOINT_MAPPER, localRegion);
    }

    public Optional<RegionReplicationCheckpoint> latestCheckpointForTarget(String localRegion, String streamName, UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT source_region, target_region, stream_name, workspace_id, last_applied_event_id, last_applied_sequence, last_applied_at, lag_millis, updated_at
                FROM region_replication_checkpoints
                WHERE target_region = ?
                  AND stream_name = ?
                  AND workspace_id = ?
                ORDER BY last_applied_at DESC NULLS LAST
                LIMIT 1
                """, CHECKPOINT_MAPPER, localRegion, streamName, workspaceId).stream().findFirst();
    }

    public void upsertReplicationCheckpoint(RegionReplicationEvent event, long lagMillis) {
        jdbcTemplate.update("""
                INSERT INTO region_replication_checkpoints (
                    source_region, target_region, stream_name, workspace_id, last_applied_event_id, last_applied_sequence, last_applied_at, lag_millis
                )
                VALUES (?, ?, ?, ?, ?, ?, now(), ?)
                ON CONFLICT (source_region, target_region, stream_name, workspace_id) DO UPDATE
                SET last_applied_event_id = EXCLUDED.last_applied_event_id,
                    last_applied_sequence = GREATEST(region_replication_checkpoints.last_applied_sequence, EXCLUDED.last_applied_sequence),
                    last_applied_at = now(),
                    lag_millis = EXCLUDED.lag_millis
                WHERE region_replication_checkpoints.last_applied_sequence <= EXCLUDED.last_applied_sequence
                """, event.sourceRegion(), event.targetRegion(), event.streamName(), event.workspaceId(), event.eventId(), event.sequence(), lagMillis);
    }

    private static WorkspaceRegionOwnership mapOwnership(ResultSet rs) throws SQLException {
        return new WorkspaceRegionOwnership(
                rs.getObject("workspace_id", UUID.class),
                rs.getString("home_region"),
                rs.getLong("ownership_epoch"),
                rs.getString("status"),
                rs.getString("transfer_state"),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getTimestamp("created_at").toInstant());
    }

    private static String string(UUID value) {
        return value == null ? null : value.toString();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static java.time.Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        var timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(java.time.Instant value) {
        return value == null ? null : Timestamp.from(value);
    }
}
