package com.kayledger.api.investigation.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kayledger.api.investigation.model.InvestigationDocument;

@Repository
public class InvestigationStore {

    private static final String DOCUMENT_SELECTS = """
            SELECT pi.workspace_id,
                   'payment_intent' AS document_type,
                   'PAYMENT_INTENT' AS reference_type,
                   pi.id AS reference_id,
                   pi.provider_profile_id,
                   pi.id AS payment_intent_id,
                   NULL::uuid AS refund_id,
                   NULL::uuid AS payout_request_id,
                   NULL::uuid AS dispute_id,
                   pi.subscription_id,
                   NULL::text AS provider_event_id,
                   pi.external_reference,
                   'PAYMENT_INTENT' AS business_reference_type,
                   pi.id AS business_reference_id,
                   pi.status,
                   NULL::text AS mismatch_type,
                   pi.currency_code,
                   pi.gross_amount_minor AS amount_minor,
                   NULL::date AS period_start,
                   NULL::date AS period_end,
                   pi.updated_at AS occurred_at
            FROM payment_intents pi
            UNION ALL
            SELECT pa.workspace_id, 'payment_attempt', 'PAYMENT_ATTEMPT', pa.id, pi.provider_profile_id, pi.id,
                   NULL::uuid, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, pa.external_reference,
                   'PAYMENT_INTENT', pi.id, pa.status, NULL::text, pi.currency_code, pa.amount_minor, NULL::date, NULL::date, pa.created_at
            FROM payment_attempts pa
            JOIN payment_intents pi ON pi.workspace_id = pa.workspace_id AND pi.id = pa.payment_intent_id
            UNION ALL
            SELECT r.workspace_id, 'refund', 'REFUND', r.id, pi.provider_profile_id, pi.id,
                   r.id, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, NULL::text,
                   'REFUND', r.id, r.status, NULL::text, pi.currency_code, r.amount_minor, NULL::date, NULL::date, r.updated_at
            FROM refunds r
            JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
            UNION ALL
            SELECT ra.workspace_id, 'refund_attempt', 'REFUND_ATTEMPT', ra.id, pi.provider_profile_id, pi.id,
                   r.id, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, ra.external_reference,
                   'REFUND', r.id, ra.status, NULL::text, pi.currency_code, r.amount_minor, NULL::date, NULL::date, ra.created_at
            FROM refund_attempts ra
            JOIN refunds r ON r.workspace_id = ra.workspace_id AND r.id = ra.refund_id
            JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
            UNION ALL
            SELECT pr.workspace_id, 'payout', 'PAYOUT_REQUEST', pr.id, pr.provider_profile_id, NULL::uuid,
                   NULL::uuid, pr.id, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'PAYOUT_REQUEST', pr.id, pr.status, NULL::text, pr.currency_code, pr.requested_amount_minor, NULL::date, NULL::date, pr.updated_at
            FROM payout_requests pr
            UNION ALL
            SELECT pa.workspace_id, 'payout_attempt', 'PAYOUT_ATTEMPT', pa.id, pr.provider_profile_id, NULL::uuid,
                   NULL::uuid, pr.id, NULL::uuid, NULL::uuid, NULL::text, pa.external_reference,
                   'PAYOUT_REQUEST', pr.id, pa.status, NULL::text, pr.currency_code, pr.requested_amount_minor, NULL::date, NULL::date, pa.created_at
            FROM payout_attempts pa
            JOIN payout_requests pr ON pr.workspace_id = pa.workspace_id AND pr.id = pa.payout_request_id
            UNION ALL
            SELECT d.workspace_id, 'dispute', 'DISPUTE', d.id, pi.provider_profile_id, pi.id,
                   NULL::uuid, NULL::uuid, d.id, pi.subscription_id, NULL::text, NULL::text,
                   'DISPUTE', d.id, d.status, NULL::text, pi.currency_code, d.disputed_amount_minor, NULL::date, NULL::date, d.updated_at
            FROM disputes d
            JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id
            UNION ALL
            SELECT pc.workspace_id, 'provider_callback', 'PROVIDER_CALLBACK', pc.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, pc.provider_event_id, NULL::text,
                   pc.business_reference_type, pc.business_reference_id, pc.processing_status, NULL::text, NULL::text, NULL::bigint, NULL::date, NULL::date, pc.updated_at
            FROM provider_callbacks pc
            UNION ALL
            SELECT s.workspace_id, 'subscription', 'SUBSCRIPTION', s.id, s.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, s.id, NULL::text, NULL::text,
                   'SUBSCRIPTION', s.id, s.status, NULL::text, NULL::text, NULL::bigint, s.current_period_start_at::date, s.current_period_end_at::date, s.updated_at
            FROM subscriptions s
            UNION ALL
            SELECT sc.workspace_id, 'subscription_cycle', 'SUBSCRIPTION_CYCLE', sc.id, sc.provider_profile_id, sc.payment_intent_id,
                   NULL::uuid, NULL::uuid, NULL::uuid, sc.subscription_id, NULL::text, sc.external_reference,
                   'SUBSCRIPTION_CYCLE', sc.id, sc.status, NULL::text, sc.currency_code, sc.gross_amount_minor, sc.cycle_start_at::date, sc.cycle_end_at::date, sc.updated_at
            FROM subscription_cycles sc
            UNION ALL
            SELECT rf.workspace_id, 'risk_flag', 'RISK_FLAG', rf.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, rf.rule_code,
                   rf.reference_type, rf.reference_id, rf.status, NULL::text, NULL::text, NULL::bigint, NULL::date, NULL::date, rf.updated_at
            FROM risk_flags rf
            UNION ALL
            SELECT rr.workspace_id, 'risk_review', 'RISK_REVIEW', rr.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'RISK_FLAG', rr.risk_flag_id, rr.status, NULL::text, NULL::text, NULL::bigint, NULL::date, NULL::date, rr.updated_at
            FROM risk_reviews rr
            UNION ALL
            SELECT rd.workspace_id, 'risk_decision', 'RISK_DECISION', rd.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   rd.reference_type, rd.reference_id, rd.outcome, NULL::text, NULL::text, NULL::bigint, NULL::date, NULL::date, rd.decided_at
            FROM risk_decisions rd
            UNION ALL
            SELECT ej.workspace_id, 'export_job', 'EXPORT_JOB', ej.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, ej.storage_key,
                   'EXPORT_JOB', ej.id, ej.status, NULL::text, NULL::text, ej.row_count::bigint, NULL::date, NULL::date, ej.updated_at
            FROM export_jobs ej
            UNION ALL
            SELECT ea.workspace_id, 'export_artifact', 'EXPORT_ARTIFACT', ea.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, ea.storage_key,
                   'EXPORT_JOB', ea.export_job_id, 'STORED', NULL::text, NULL::text, ea.byte_size::bigint, NULL::date, NULL::date, ea.created_at
            FROM export_artifacts ea
            UNION ALL
            SELECT prr.workspace_id, 'provider_reconciliation_run', 'PROVIDER_RECONCILIATION_RUN', prr.id, prr.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, prr.source_reference,
                   'PROVIDER_TRUTH_IMPORT', prr.truth_import_id, prr.status, NULL::text, prr.currency_code, prr.unresolved_item_count::bigint, prr.statement_period_start, prr.statement_period_end, prr.updated_at
            FROM provider_reconciliation_runs prr
            UNION ALL
            SELECT pri.workspace_id, 'provider_reconciliation_item', 'PROVIDER_RECONCILIATION_ITEM', pri.id, pri.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, pri.source_reference,
                   'PROVIDER_RECONCILIATION_RUN', pri.reconciliation_run_id, pri.status, pri.mismatch_type, pri.currency_code, NULL::bigint, NULL::date, NULL::date, pri.updated_at
            FROM provider_reconciliation_items pri
            UNION ALL
            SELECT fap.workspace_id, 'accounting_period', 'ACCOUNTING_PERIOD', fap.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'ACCOUNTING_PERIOD', fap.id, fap.status, NULL::text, NULL::text, NULL::bigint, fap.period_start, fap.period_end, fap.updated_at
            FROM financial_accounting_periods fap
            UNION ALL
            SELECT fps.workspace_id, 'finalized_provider_statement', 'FINALIZED_PROVIDER_STATEMENT', fps.id, fps.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'ACCOUNTING_PERIOD', fps.accounting_period_id, fps.status, NULL::text, fps.currency_code, fps.net_earnings_amount_minor, fps.period_start, fps.period_end, fps.updated_at
            FROM finalized_provider_statements fps
            UNION ALL
            SELECT fcae.workspace_id, 'financial_close_audit_event', 'FINANCIAL_CLOSE_AUDIT_EVENT', fcae.id, fps.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   COALESCE(CASE WHEN fcae.finalized_statement_id IS NOT NULL THEN 'FINALIZED_PROVIDER_STATEMENT' END, 'ACCOUNTING_PERIOD'),
                   COALESCE(fcae.finalized_statement_id, fcae.accounting_period_id), fcae.event_type, NULL::text, fps.currency_code, NULL::bigint, fap.period_start, fap.period_end, fcae.created_at
            FROM financial_close_audit_events fcae
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = fcae.workspace_id AND fap.id = fcae.accounting_period_id
            LEFT JOIN finalized_provider_statements fps ON fps.workspace_id = fcae.workspace_id AND fps.id = fcae.finalized_statement_id
            UNION ALL
            SELECT far.workspace_id, 'financial_approval_request', 'FINANCIAL_APPROVAL_REQUEST', far.id, far.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, far.action_type,
                   far.target_type, far.target_id, far.status, far.action_type, far.currency_code, far.amount_minor, NULL::date, NULL::date, far.updated_at
            FROM financial_approval_requests far
            UNION ALL
            SELECT fad.workspace_id, 'financial_approval_decision', 'FINANCIAL_APPROVAL_DECISION', fad.id, far.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, far.action_type,
                   'FINANCIAL_APPROVAL_REQUEST', far.id, fad.decision, far.action_type, far.currency_code, far.amount_minor, NULL::date, NULL::date, fad.created_at
            FROM financial_approval_decisions fad
            JOIN financial_approval_requests far ON far.workspace_id = fad.workspace_id AND far.id = fad.approval_request_id
            UNION ALL
            SELECT faes.workspace_id, 'financial_approval_execution', 'FINANCIAL_APPROVAL_EXECUTION', faes.id, far.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, far.action_type,
                   'FINANCIAL_APPROVAL_REQUEST', far.id, faes.execution_status,
                   CASE
                       WHEN faes.execution_status = 'IN_PROGRESS'
                            AND faes.execution_lease_expires_at IS NOT NULL
                            AND faes.execution_lease_expires_at <= now() THEN 'STALE_APPROVAL_EXECUTION'
                       WHEN faes.execution_status = 'FAILED' THEN 'FAILED_APPROVAL_EXECUTION'
                       ELSE far.action_type
                   END,
                   far.currency_code, far.amount_minor, NULL::date, NULL::date, faes.updated_at
            FROM financial_approval_execution_state faes
            JOIN financial_approval_requests far ON far.workspace_id = faes.workspace_id AND far.id = faes.approval_request_id
            UNION ALL
            SELECT fep.workspace_id, 'finance_evidence_pack', 'FINANCE_EVIDENCE_PACK', fep.id,
                   COALESCE(fps.provider_profile_id, prr.provider_profile_id, pti.provider_profile_id, far.provider_profile_id), NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, fep.source_reference,
                   fep.evidence_pack_type, COALESCE(fep.finalized_statement_id, fep.reconciliation_run_id, fep.provider_truth_import_id, fep.approval_request_id, fep.accounting_period_id),
                   fep.status, fep.evidence_pack_type, COALESCE(fps.currency_code, prr.currency_code, pti.currency_code, far.currency_code), NULL::bigint,
                   COALESCE(fap.period_start, prr.statement_period_start, pti.statement_period_start),
                   COALESCE(fap.period_end, prr.statement_period_end, pti.statement_period_end),
                   fep.updated_at
            FROM finance_evidence_packs fep
            LEFT JOIN finalized_provider_statements fps ON fps.workspace_id = fep.workspace_id AND fps.id = fep.finalized_statement_id
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = fep.workspace_id AND fap.id = fep.accounting_period_id
            LEFT JOIN provider_reconciliation_runs prr ON prr.workspace_id = fep.workspace_id AND prr.id = fep.reconciliation_run_id
            LEFT JOIN provider_reconciliation_truth_imports pti ON pti.workspace_id = fep.workspace_id AND pti.id = fep.provider_truth_import_id
            LEFT JOIN financial_approval_requests far ON far.workspace_id = fep.workspace_id AND far.id = fep.approval_request_id
            UNION ALL
            SELECT fee.workspace_id, 'finance_evidence_export', 'FINANCE_EVIDENCE_EXPORT', fee.id,
                   COALESCE(fps.provider_profile_id, prr.provider_profile_id, pti.provider_profile_id, far.provider_profile_id), NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, fee.artifact_reference,
                   'FINANCE_EVIDENCE_PACK', fee.evidence_pack_id, fee.export_status, fep.evidence_pack_type,
                   COALESCE(fps.currency_code, prr.currency_code, pti.currency_code, far.currency_code), fee.artifact_size_bytes,
                   COALESCE(fap.period_start, prr.statement_period_start, pti.statement_period_start),
                   COALESCE(fap.period_end, prr.statement_period_end, pti.statement_period_end),
                   fee.created_at
            FROM finance_evidence_exports fee
            JOIN finance_evidence_packs fep ON fep.workspace_id = fee.workspace_id AND fep.id = fee.evidence_pack_id
            LEFT JOIN finalized_provider_statements fps ON fps.workspace_id = fep.workspace_id AND fps.id = fep.finalized_statement_id
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = fep.workspace_id AND fap.id = fep.accounting_period_id
            LEFT JOIN provider_reconciliation_runs prr ON prr.workspace_id = fep.workspace_id AND prr.id = fep.reconciliation_run_id
            LEFT JOIN provider_reconciliation_truth_imports pti ON pti.workspace_id = fep.workspace_id AND pti.id = fep.provider_truth_import_id
            LEFT JOIN financial_approval_requests far ON far.workspace_id = fep.workspace_id AND far.id = fep.approval_request_id
            UNION ALL
            SELECT fea.workspace_id, 'finance_evidence_artifact', 'FINANCE_EVIDENCE_ARTIFACT', fea.id,
                   COALESCE(fps.provider_profile_id, prr.provider_profile_id, pti.provider_profile_id, far.provider_profile_id), NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, fea.checksum_value, fea.id::text,
                   'FINANCE_EVIDENCE_EXPORT', fee.id, 'STORED', fep.evidence_pack_type,
                   COALESCE(fps.currency_code, prr.currency_code, pti.currency_code, far.currency_code), fea.artifact_size_bytes,
                   COALESCE(fap.period_start, prr.statement_period_start, pti.statement_period_start),
                   COALESCE(fap.period_end, prr.statement_period_end, pti.statement_period_end),
                   fea.created_at
            FROM finance_evidence_export_artifacts fea
            JOIN finance_evidence_exports fee ON fee.workspace_id = fea.workspace_id AND fee.artifact_id = fea.id
            JOIN finance_evidence_packs fep ON fep.workspace_id = fee.workspace_id AND fep.id = fee.evidence_pack_id
            LEFT JOIN finalized_provider_statements fps ON fps.workspace_id = fep.workspace_id AND fps.id = fep.finalized_statement_id
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = fep.workspace_id AND fap.id = fep.accounting_period_id
            LEFT JOIN provider_reconciliation_runs prr ON prr.workspace_id = fep.workspace_id AND prr.id = fep.reconciliation_run_id
            LEFT JOIN provider_reconciliation_truth_imports pti ON pti.workspace_id = fep.workspace_id AND pti.id = fep.provider_truth_import_id
            LEFT JOIN financial_approval_requests far ON far.workspace_id = fep.workspace_id AND far.id = fep.approval_request_id
            UNION ALL
            SELECT mfe.workspace_id, 'merchant_finance_event', 'MERCHANT_FINANCE_EVENT', mfe.id, mfe.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, mfe.event_key,
                   mfe.source_reference_type, mfe.source_reference_id, mfe.event_type, mfe.event_type, mfe.currency_code, NULL::bigint, fap.period_start, fap.period_end, mfe.occurred_at
            FROM merchant_finance_events mfe
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = mfe.workspace_id AND fap.id = mfe.accounting_period_id
            UNION ALL
            SELECT mfed.workspace_id, 'merchant_finance_event_delivery', 'MERCHANT_FINANCE_EVENT_DELIVERY', mfed.id, mfe.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, mfed.dedupe_key, mfed.endpoint_id::text,
                   'MERCHANT_FINANCE_EVENT', mfed.merchant_finance_event_id, mfed.delivery_status,
                   CASE
                       WHEN mfed.delivery_status = 'CLAIMED'
                            AND mfed.claim_expires_at IS NOT NULL
                            AND mfed.claim_expires_at <= now() THEN 'STALE_CLAIMED_DELIVERY'
                       WHEN mfed.delivery_status = 'PARKED' THEN 'PARKED_DELIVERY'
                       WHEN mfed.delivery_status = 'FAILED' THEN 'FAILED_DELIVERY'
                       ELSE mfe.event_type
                   END,
                   mfe.currency_code, NULL::bigint, fap.period_start, fap.period_end, mfed.updated_at
            FROM merchant_finance_event_deliveries mfed
            JOIN merchant_finance_events mfe ON mfe.workspace_id = mfed.workspace_id AND mfe.id = mfed.merchant_finance_event_id
            LEFT JOIN financial_accounting_periods fap ON fap.workspace_id = mfe.workspace_id AND fap.id = mfe.accounting_period_id
            """;

    private final JdbcTemplate jdbcTemplate;

    public InvestigationStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ReindexJob createReindexJob(UUID workspaceId, UUID requestedByActorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO investigation_reindex_jobs (workspace_id, requested_by_actor_id)
                VALUES (?, ?)
                RETURNING *
                """, this::mapReindexJob, workspaceId, requestedByActorId);
    }

    public ReindexJob attachReindexWorkflow(UUID workspaceId, UUID jobId, String workflowId, String runId) {
        return jdbcTemplate.queryForObject("""
                UPDATE investigation_reindex_jobs
                SET temporal_workflow_id = ?,
                    temporal_run_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, this::mapReindexJob, workflowId, runId, workspaceId, jobId);
    }

    public ReindexJob markReindexRunning(UUID workspaceId, UUID jobId) {
        return jdbcTemplate.queryForObject("""
                UPDATE investigation_reindex_jobs
                SET status = 'RUNNING',
                    started_at = COALESCE(started_at, now()),
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('REQUESTED', 'RUNNING')
                RETURNING *
                """, this::mapReindexJob, workspaceId, jobId);
    }

    public ReindexJob markReindexSucceeded(UUID workspaceId, UUID jobId, int indexed, int failed) {
        return jdbcTemplate.queryForObject("""
                UPDATE investigation_reindex_jobs
                SET status = 'SUCCEEDED',
                    started_at = COALESCE(started_at, now()),
                    indexed_count = ?,
                    failed_count = ?,
                    completed_at = now(),
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, this::mapReindexJob, indexed, failed, workspaceId, jobId);
    }

    public ReindexJob markReindexFailed(UUID workspaceId, UUID jobId, int indexed, int failed, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE investigation_reindex_jobs
                SET status = 'FAILED',
                    indexed_count = ?,
                    failed_count = ?,
                    completed_at = now(),
                    failure_reason = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING *
                """, this::mapReindexJob, indexed, failed, truncate(failureReason), workspaceId, jobId);
    }

    public List<InvestigationDocument> documentsForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM (
                """ + DOCUMENT_SELECTS + """
                ) investigation_documents
                WHERE workspace_id = ?
                """, this::mapDocument, workspaceId);
    }

    public List<InvestigationDocument> documentsForReference(UUID workspaceId, String referenceType, UUID referenceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM (
                """ + DOCUMENT_SELECTS + """
                ) investigation_documents
                WHERE workspace_id = ?
                  AND (
                    (reference_type = ? AND reference_id = ?)
                    OR (business_reference_type = ? AND business_reference_id = ?)
                  )
                """, this::mapDocument, workspaceId, referenceType, referenceId, referenceType, referenceId);
    }

    public void recordIndexed(InvestigationDocument document) {
        jdbcTemplate.update("""
                INSERT INTO operator_search_index_state (
                    workspace_id, document_type, reference_type, reference_id,
                    search_document_id, source_updated_at, indexed_at, status, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, now(), 'INDEXED', NULL)
                ON CONFLICT (workspace_id, document_type, reference_id) DO UPDATE
                SET search_document_id = EXCLUDED.search_document_id,
                    source_updated_at = EXCLUDED.source_updated_at,
                    indexed_at = now(),
                    status = 'INDEXED',
                    last_error = NULL
                """,
                document.workspaceId(),
                document.documentType(),
                document.referenceType(),
                document.referenceId(),
                document.documentId(),
                timestamp(document.occurredAt()));
    }

    public void recordFailed(InvestigationDocument document, Exception exception) {
        jdbcTemplate.update("""
                INSERT INTO operator_search_index_state (
                    workspace_id, document_type, reference_type, reference_id,
                    search_document_id, source_updated_at, status, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, 'FAILED', ?)
                ON CONFLICT (workspace_id, document_type, reference_id) DO UPDATE
                SET source_updated_at = EXCLUDED.source_updated_at,
                    status = 'FAILED',
                    last_error = EXCLUDED.last_error
                """,
                document.workspaceId(),
                document.documentType(),
                document.referenceType(),
                document.referenceId(),
                document.documentId(),
                timestamp(document.occurredAt()),
                truncate(exception.getMessage()));
    }

    private InvestigationDocument mapDocument(ResultSet rs, int rowNum) throws SQLException {
        UUID workspaceId = rs.getObject("workspace_id", UUID.class);
        String documentType = rs.getString("document_type");
        String referenceType = rs.getString("reference_type");
        UUID referenceId = rs.getObject("reference_id", UUID.class);
        UUID providerProfileId = rs.getObject("provider_profile_id", UUID.class);
        UUID paymentIntentId = rs.getObject("payment_intent_id", UUID.class);
        UUID refundId = rs.getObject("refund_id", UUID.class);
        UUID payoutRequestId = rs.getObject("payout_request_id", UUID.class);
        UUID disputeId = rs.getObject("dispute_id", UUID.class);
        UUID subscriptionId = rs.getObject("subscription_id", UUID.class);
        Instant occurredAt = instant(rs, "occurred_at");
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("documentType", documentType);
        data.put("referenceType", referenceType);
        data.put("referenceId", referenceId);
        data.put("providerProfileId", providerProfileId);
        data.put("paymentIntentId", paymentIntentId);
        data.put("refundId", refundId);
        data.put("payoutRequestId", payoutRequestId);
        data.put("disputeId", disputeId);
        data.put("subscriptionId", subscriptionId);
        data.put("providerEventId", rs.getString("provider_event_id"));
        data.put("externalReference", rs.getString("external_reference"));
        data.put("businessReferenceType", rs.getString("business_reference_type"));
        data.put("businessReferenceId", rs.getObject("business_reference_id", UUID.class));
        data.put("status", rs.getString("status"));
        data.put("mismatchType", rs.getString("mismatch_type"));
        data.put("currencyCode", rs.getString("currency_code"));
        data.put("amountMinor", nullableLong(rs, "amount_minor"));
        data.put("periodStart", string(rs, "period_start"));
        data.put("periodEnd", string(rs, "period_end"));
        data.put("occurredAt", occurredAt);
        return new InvestigationDocument(
                documentType + ":" + referenceId,
                workspaceId,
                documentType,
                referenceType,
                referenceId,
                providerProfileId,
                paymentIntentId,
                refundId,
                payoutRequestId,
                disputeId,
                subscriptionId,
                rs.getString("provider_event_id"),
                rs.getString("external_reference"),
                rs.getString("business_reference_type"),
                rs.getObject("business_reference_id", UUID.class),
                rs.getString("status"),
                rs.getString("mismatch_type"),
                rs.getString("currency_code"),
                nullableLong(rs, "amount_minor"),
                string(rs, "period_start"),
                string(rs, "period_end"),
                occurredAt,
                data);
    }

    private ReindexJob mapReindexJob(ResultSet rs, int rowNum) throws SQLException {
        return new ReindexJob(
                rs.getObject("id", UUID.class),
                rs.getObject("workspace_id", UUID.class),
                rs.getString("status"),
                rs.getString("trigger_mode"),
                rs.getString("temporal_workflow_id"),
                rs.getString("temporal_run_id"),
                rs.getObject("requested_by_actor_id", UUID.class),
                rs.getInt("indexed_count"),
                rs.getInt("failed_count"),
                rs.getString("failure_reason"),
                instant(rs, "requested_at"),
                instant(rs, "started_at"),
                instant(rs, "completed_at"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    public record ReindexJob(
            UUID id,
            UUID workspaceId,
            String status,
            String triggerMode,
            String temporalWorkflowId,
            String temporalRunId,
            UUID requestedByActorId,
            int indexedCount,
            int failedCount,
            String failureReason,
            Instant requestedAt,
            Instant startedAt,
            Instant completedAt,
            Instant createdAt,
            Instant updatedAt) {
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static String string(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value == null ? null : value.toString();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }
}
