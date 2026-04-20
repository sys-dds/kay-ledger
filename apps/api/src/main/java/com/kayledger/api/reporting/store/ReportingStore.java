package com.kayledger.api.reporting.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.reporting.model.ExportArtifact;
import com.kayledger.api.reporting.model.ExportJob;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Repository
public class ReportingStore {

    private static final RowMapper<ProviderFinancialSummary> SUMMARY_MAPPER = (rs, rowNum) -> new ProviderFinancialSummary(
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
            instant(rs, "refreshed_at"));

    private static final RowMapper<ExportJob> JOB_MAPPER = (rs, rowNum) -> new ExportJob(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("export_type"),
            rs.getString("status"),
            rs.getString("generation_mode"),
            rs.getString("temporal_workflow_id"),
            rs.getString("temporal_run_id"),
            rs.getString("trigger_mode"),
            rs.getObject("requested_by_actor_id", UUID.class),
            rs.getString("parameters_json"),
            rs.getInt("row_count"),
            rs.getString("storage_key"),
            rs.getString("content_type"),
            rs.getString("failure_reason"),
            instant(rs, "requested_at"),
            nullableInstant(rs, "orchestration_started_at"),
            nullableInstant(rs, "orchestration_completed_at"),
            nullableInstant(rs, "completed_at"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ExportArtifact> ARTIFACT_MAPPER = (rs, rowNum) -> new ExportArtifact(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("export_job_id", UUID.class),
            rs.getString("storage_key"),
            rs.getString("content_type"),
            rs.getLong("byte_size"),
            rs.getInt("row_count"),
            rs.getString("checksum_sha256"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public ReportingStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int refreshProviderSummaries(UUID workspaceId) {
        jdbcTemplate.update("DELETE FROM financial_provider_summaries WHERE workspace_id = ?", workspaceId);
        return jdbcTemplate.update("""
                INSERT INTO financial_provider_summaries (
                    workspace_id, provider_profile_id, currency_code,
                    settled_gross_amount_minor, fee_amount_minor, net_earnings_amount_minor,
                    current_payout_requested_amount_minor, payout_succeeded_amount_minor,
                    refund_amount_minor, active_dispute_exposure_amount_minor, settled_subscription_net_revenue_amount_minor
                )
                WITH provider_currency AS (
                    SELECT DISTINCT workspace_id, provider_profile_id, currency_code
                    FROM payment_intents
                    WHERE workspace_id = ?
                    UNION
                    SELECT DISTINCT workspace_id, provider_profile_id, currency_code
                    FROM payout_requests
                    WHERE workspace_id = ?
                )
                SELECT pc.workspace_id,
                       pc.provider_profile_id,
                       pc.currency_code,
                       COALESCE((SELECT SUM(gross_amount_minor) FROM payment_intents pi
                           WHERE pi.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND pi.status = 'SETTLED'), 0),
                       COALESCE((SELECT SUM(fee_amount_minor) FROM payment_intents pi
                           WHERE pi.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND pi.status = 'SETTLED'), 0),
                       COALESCE((SELECT SUM(net_amount_minor) FROM payment_intents pi
                           WHERE pi.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND pi.status = 'SETTLED'), 0),
                       COALESCE((SELECT SUM(requested_amount_minor) FROM payout_requests pr
                           WHERE pr.workspace_id = pc.workspace_id AND pr.provider_profile_id = pc.provider_profile_id
                             AND pr.currency_code = pc.currency_code AND pr.status IN ('REQUESTED', 'PROCESSING')), 0),
                       COALESCE((SELECT SUM(requested_amount_minor) FROM payout_requests pr
                           WHERE pr.workspace_id = pc.workspace_id AND pr.provider_profile_id = pc.provider_profile_id
                             AND pr.currency_code = pc.currency_code AND pr.status = 'SUCCEEDED'), 0),
                       COALESCE((SELECT SUM(r.amount_minor) FROM refunds r
                           JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
                           WHERE r.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND r.status = 'SUCCEEDED'), 0),
                       COALESCE((SELECT SUM(d.disputed_amount_minor) FROM disputes d
                           JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id
                           WHERE d.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND d.status = 'OPEN'), 0),
                       COALESCE((SELECT SUM(pi.net_amount_minor) FROM payment_intents pi
                           WHERE pi.workspace_id = pc.workspace_id AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code AND pi.subscription_id IS NOT NULL AND pi.status = 'SETTLED'), 0)
                FROM provider_currency pc
                """, workspaceId, workspaceId);
    }

    public List<ProviderFinancialSummary> listProviderSummaries(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM financial_provider_summaries
                WHERE workspace_id = ?
                ORDER BY provider_profile_id, currency_code
                """, SUMMARY_MAPPER, workspaceId);
    }

    public Optional<ProviderFinancialSummary> providerSummaryForPeriod(UUID workspaceId, UUID providerProfileId, String currencyCode, LocalDate periodStart, LocalDate periodEnd) {
        return jdbcTemplate.query("""
                WITH bounds AS (
                    SELECT ?::date AS period_start, (?::date + interval '1 day') AS period_end
                ),
                provider_currency AS (
                    SELECT 1
                    WHERE EXISTS (
                        SELECT 1
                        FROM payment_intents pi, bounds b
                        WHERE pi.workspace_id = ?
                          AND pi.provider_profile_id = ?
                          AND pi.currency_code = ?
                          AND pi.status = 'SETTLED'
                          AND pi.updated_at >= b.period_start
                          AND pi.updated_at < b.period_end
                        UNION ALL
                        SELECT 1
                        FROM payout_requests pr, bounds b
                        WHERE pr.workspace_id = ?
                          AND pr.provider_profile_id = ?
                          AND pr.currency_code = ?
                          AND pr.status = 'SUCCEEDED'
                          AND pr.updated_at >= b.period_start
                          AND pr.updated_at < b.period_end
                        UNION ALL
                        SELECT 1
                        FROM refunds r
                        JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id,
                             bounds b
                        WHERE r.workspace_id = ?
                          AND pi.provider_profile_id = ?
                          AND pi.currency_code = ?
                          AND r.status = 'SUCCEEDED'
                          AND r.updated_at >= b.period_start
                          AND r.updated_at < b.period_end
                        UNION ALL
                        SELECT 1
                        FROM disputes d
                        JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id,
                             bounds b
                        WHERE d.workspace_id = ?
                          AND pi.provider_profile_id = ?
                          AND pi.currency_code = ?
                          AND d.status = 'OPEN'
                          AND d.updated_at >= b.period_start
                          AND d.updated_at < b.period_end
                    )
                )
                SELECT ?::uuid AS workspace_id,
                       ?::uuid AS provider_profile_id,
                       ?::char(3) AS currency_code,
                       COALESCE((SELECT SUM(gross_amount_minor) FROM payment_intents pi, bounds b
                           WHERE pi.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND pi.status = 'SETTLED'
                             AND pi.updated_at >= b.period_start AND pi.updated_at < b.period_end), 0) AS settled_gross_amount_minor,
                       COALESCE((SELECT SUM(fee_amount_minor) FROM payment_intents pi, bounds b
                           WHERE pi.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND pi.status = 'SETTLED'
                             AND pi.updated_at >= b.period_start AND pi.updated_at < b.period_end), 0) AS fee_amount_minor,
                       COALESCE((SELECT SUM(net_amount_minor) FROM payment_intents pi, bounds b
                           WHERE pi.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND pi.status = 'SETTLED'
                             AND pi.updated_at >= b.period_start AND pi.updated_at < b.period_end), 0) AS net_earnings_amount_minor,
                       COALESCE((SELECT SUM(requested_amount_minor) FROM payout_requests pr, bounds b
                           WHERE pr.workspace_id = ? AND pr.provider_profile_id = ?
                             AND pr.currency_code = ? AND pr.status IN ('REQUESTED', 'PROCESSING')
                             AND pr.updated_at >= b.period_start AND pr.updated_at < b.period_end), 0) AS current_payout_requested_amount_minor,
                       COALESCE((SELECT SUM(requested_amount_minor) FROM payout_requests pr, bounds b
                           WHERE pr.workspace_id = ? AND pr.provider_profile_id = ?
                             AND pr.currency_code = ? AND pr.status = 'SUCCEEDED'
                             AND pr.updated_at >= b.period_start AND pr.updated_at < b.period_end), 0) AS payout_succeeded_amount_minor,
                       COALESCE((SELECT SUM(r.amount_minor) FROM refunds r
                           JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id,
                                bounds b
                           WHERE r.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND r.status = 'SUCCEEDED'
                             AND r.updated_at >= b.period_start AND r.updated_at < b.period_end), 0) AS refund_amount_minor,
                       COALESCE((SELECT SUM(d.disputed_amount_minor) FROM disputes d
                           JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id,
                                bounds b
                           WHERE d.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND d.status = 'OPEN'
                             AND d.updated_at >= b.period_start AND d.updated_at < b.period_end), 0) AS active_dispute_exposure_amount_minor,
                       COALESCE((SELECT SUM(pi.net_amount_minor) FROM payment_intents pi, bounds b
                           WHERE pi.workspace_id = ? AND pi.provider_profile_id = ?
                             AND pi.currency_code = ? AND pi.subscription_id IS NOT NULL AND pi.status = 'SETTLED'
                             AND pi.updated_at >= b.period_start AND pi.updated_at < b.period_end), 0) AS settled_subscription_net_revenue_amount_minor,
                       now() AS refreshed_at
                FROM provider_currency pc
                """, SUMMARY_MAPPER,
                periodStart, periodEnd,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode).stream().findFirst();
    }

    public List<ProviderFinancialSummary> listProviderSummariesForPeriod(UUID workspaceId, LocalDate periodStart, LocalDate periodEnd) {
        return jdbcTemplate.query("""
                WITH bounds AS (
                    SELECT ?::date AS period_start, (?::date + interval '1 day') AS period_end
                )
                SELECT DISTINCT provider_profile_id, currency_code
                FROM (
                    SELECT pi.provider_profile_id, pi.currency_code
                    FROM payment_intents pi, bounds b
                    WHERE pi.workspace_id = ?
                      AND pi.status = 'SETTLED'
                      AND pi.updated_at >= b.period_start
                      AND pi.updated_at < b.period_end
                    UNION
                    SELECT pr.provider_profile_id, pr.currency_code
                    FROM payout_requests pr, bounds b
                    WHERE pr.workspace_id = ?
                      AND pr.updated_at >= b.period_start
                      AND pr.updated_at < b.period_end
                    UNION
                    SELECT pi.provider_profile_id, pi.currency_code
                    FROM refunds r
                    JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id,
                         bounds b
                    WHERE r.workspace_id = ?
                      AND r.updated_at >= b.period_start
                      AND r.updated_at < b.period_end
                    UNION
                    SELECT pi.provider_profile_id, pi.currency_code
                    FROM disputes d
                    JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id,
                         bounds b
                    WHERE d.workspace_id = ?
                      AND d.updated_at >= b.period_start
                      AND d.updated_at < b.period_end
                ) provider_currency
                ORDER BY provider_profile_id, currency_code
                """, (rs, rowNum) -> new ProviderCurrency(
                rs.getObject("provider_profile_id", UUID.class),
                rs.getString("currency_code")), periodStart, periodEnd, workspaceId, workspaceId, workspaceId, workspaceId)
                .stream()
                .map(providerCurrency -> providerSummaryForPeriod(workspaceId, providerCurrency.providerProfileId(), providerCurrency.currencyCode(), periodStart, periodEnd))
                .flatMap(Optional::stream)
                .toList();
    }

    public ExportJob createExportJob(UUID workspaceId, String exportType, UUID requestedByActorId, String parametersJson) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO export_jobs (workspace_id, export_type, requested_by_actor_id, parameters_json)
                VALUES (?, ?, ?, ?::jsonb)
                RETURNING *
                """, JOB_MAPPER, workspaceId, exportType, requestedByActorId, parametersJson);
    }

    public ExportJob createAsyncExportJob(UUID workspaceId, String exportType, UUID requestedByActorId, String parametersJson) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO export_jobs (workspace_id, export_type, requested_by_actor_id, parameters_json, generation_mode)
                VALUES (?, ?, ?, ?::jsonb, 'TEMPORAL_ASYNC')
                RETURNING *
                """, JOB_MAPPER, workspaceId, exportType, requestedByActorId, parametersJson);
    }

    public ExportJob attachWorkflow(UUID workspaceId, UUID jobId, String workflowId, String runId) {
        return jdbcTemplate.queryForObject("""
                UPDATE export_jobs
                SET temporal_workflow_id = ?,
                    temporal_run_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, JOB_MAPPER, workflowId, runId, workspaceId, jobId);
    }

    public ExportJob markRunning(UUID workspaceId, UUID jobId) {
        return jdbcTemplate.queryForObject("""
                UPDATE export_jobs
                SET status = 'RUNNING'
                    , orchestration_started_at = COALESCE(orchestration_started_at, now())
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'REQUESTED'
                RETURNING *
                """, JOB_MAPPER, workspaceId, jobId);
    }

    public ExportJob markSucceeded(UUID workspaceId, UUID jobId, int rowCount, String storageKey, String contentType) {
        return jdbcTemplate.queryForObject("""
                UPDATE export_jobs
                SET status = 'SUCCEEDED',
                    orchestration_started_at = COALESCE(orchestration_started_at, now()),
                    row_count = ?,
                    storage_key = ?,
                    content_type = ?,
                    completed_at = now(),
                    orchestration_completed_at = now(),
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, JOB_MAPPER, rowCount, storageKey, contentType, workspaceId, jobId);
    }

    public ExportJob markFailed(UUID workspaceId, UUID jobId, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE export_jobs
                SET status = 'FAILED',
                    failure_reason = ?,
                    completed_at = now(),
                    orchestration_completed_at = now()
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, JOB_MAPPER, failureReason, workspaceId, jobId);
    }

    public ExportArtifact createArtifact(UUID workspaceId, UUID jobId, String storageKey, String contentType, long byteSize, int rowCount, String checksumSha256) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO export_artifacts (
                    workspace_id, export_job_id, storage_key, content_type, byte_size, row_count, checksum_sha256
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, ARTIFACT_MAPPER, workspaceId, jobId, storageKey, contentType, byteSize, rowCount, checksumSha256);
    }

    public List<ExportJob> listJobs(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM export_jobs
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, JOB_MAPPER, workspaceId);
    }

    public ExportJob findJob(UUID workspaceId, UUID jobId) {
        return jdbcTemplate.queryForObject("""
                SELECT *
                FROM export_jobs
                WHERE workspace_id = ?
                  AND id = ?
                """, JOB_MAPPER, workspaceId, jobId);
    }

    public List<ExportArtifact> listArtifacts(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM export_artifacts
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, ARTIFACT_MAPPER, workspaceId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private record ProviderCurrency(UUID providerProfileId, String currencyCode) {
    }
}
