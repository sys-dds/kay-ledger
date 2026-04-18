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
                   pi.currency_code,
                   pi.gross_amount_minor AS amount_minor,
                   pi.updated_at AS occurred_at
            FROM payment_intents pi
            UNION ALL
            SELECT pa.workspace_id, 'payment_attempt', 'PAYMENT_ATTEMPT', pa.id, pi.provider_profile_id, pi.id,
                   NULL::uuid, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, pa.external_reference,
                   'PAYMENT_INTENT', pi.id, pa.status, pi.currency_code, pa.amount_minor, pa.created_at
            FROM payment_attempts pa
            JOIN payment_intents pi ON pi.workspace_id = pa.workspace_id AND pi.id = pa.payment_intent_id
            UNION ALL
            SELECT r.workspace_id, 'refund', 'REFUND', r.id, pi.provider_profile_id, pi.id,
                   r.id, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, NULL::text,
                   'REFUND', r.id, r.status, pi.currency_code, r.amount_minor, r.updated_at
            FROM refunds r
            JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
            UNION ALL
            SELECT ra.workspace_id, 'refund_attempt', 'REFUND_ATTEMPT', ra.id, pi.provider_profile_id, pi.id,
                   r.id, NULL::uuid, NULL::uuid, pi.subscription_id, NULL::text, ra.external_reference,
                   'REFUND', r.id, ra.status, pi.currency_code, r.amount_minor, ra.created_at
            FROM refund_attempts ra
            JOIN refunds r ON r.workspace_id = ra.workspace_id AND r.id = ra.refund_id
            JOIN payment_intents pi ON pi.workspace_id = r.workspace_id AND pi.id = r.payment_intent_id
            UNION ALL
            SELECT pr.workspace_id, 'payout', 'PAYOUT_REQUEST', pr.id, pr.provider_profile_id, NULL::uuid,
                   NULL::uuid, pr.id, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'PAYOUT_REQUEST', pr.id, pr.status, pr.currency_code, pr.requested_amount_minor, pr.updated_at
            FROM payout_requests pr
            UNION ALL
            SELECT pa.workspace_id, 'payout_attempt', 'PAYOUT_ATTEMPT', pa.id, pr.provider_profile_id, NULL::uuid,
                   NULL::uuid, pr.id, NULL::uuid, NULL::uuid, NULL::text, pa.external_reference,
                   'PAYOUT_REQUEST', pr.id, pa.status, pr.currency_code, pr.requested_amount_minor, pa.created_at
            FROM payout_attempts pa
            JOIN payout_requests pr ON pr.workspace_id = pa.workspace_id AND pr.id = pa.payout_request_id
            UNION ALL
            SELECT d.workspace_id, 'dispute', 'DISPUTE', d.id, pi.provider_profile_id, pi.id,
                   NULL::uuid, NULL::uuid, d.id, pi.subscription_id, NULL::text, NULL::text,
                   'DISPUTE', d.id, d.status, pi.currency_code, d.disputed_amount_minor, d.updated_at
            FROM disputes d
            JOIN payment_intents pi ON pi.workspace_id = d.workspace_id AND pi.id = d.payment_intent_id
            UNION ALL
            SELECT pc.workspace_id, 'provider_callback', 'PROVIDER_CALLBACK', pc.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, pc.provider_event_id, NULL::text,
                   pc.business_reference_type, pc.business_reference_id, pc.processing_status, NULL::text, NULL::bigint, pc.updated_at
            FROM provider_callbacks pc
            UNION ALL
            SELECT rm.workspace_id, 'reconciliation_mismatch', 'RECONCILIATION_MISMATCH', rm.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   rm.business_reference_type, rm.business_reference_id, rm.repair_status, NULL::text, NULL::bigint, rm.updated_at
            FROM reconciliation_mismatches rm
            UNION ALL
            SELECT s.workspace_id, 'subscription', 'SUBSCRIPTION', s.id, s.provider_profile_id, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, s.id, NULL::text, NULL::text,
                   'SUBSCRIPTION', s.id, s.status, NULL::text, NULL::bigint, s.updated_at
            FROM subscriptions s
            UNION ALL
            SELECT sc.workspace_id, 'subscription_cycle', 'SUBSCRIPTION_CYCLE', sc.id, sc.provider_profile_id, sc.payment_intent_id,
                   NULL::uuid, NULL::uuid, NULL::uuid, sc.subscription_id, NULL::text, sc.external_reference,
                   'SUBSCRIPTION_CYCLE', sc.id, sc.status, sc.currency_code, sc.gross_amount_minor, sc.updated_at
            FROM subscription_cycles sc
            UNION ALL
            SELECT rf.workspace_id, 'risk_flag', 'RISK_FLAG', rf.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, rf.rule_code,
                   rf.reference_type, rf.reference_id, rf.status, NULL::text, NULL::bigint, rf.updated_at
            FROM risk_flags rf
            UNION ALL
            SELECT rr.workspace_id, 'risk_review', 'RISK_REVIEW', rr.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   'RISK_FLAG', rr.risk_flag_id, rr.status, NULL::text, NULL::bigint, rr.updated_at
            FROM risk_reviews rr
            UNION ALL
            SELECT rd.workspace_id, 'risk_decision', 'RISK_DECISION', rd.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::text,
                   rd.reference_type, rd.reference_id, rd.outcome, NULL::text, NULL::bigint, rd.decided_at
            FROM risk_decisions rd
            UNION ALL
            SELECT ej.workspace_id, 'export_job', 'EXPORT_JOB', ej.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, ej.storage_key,
                   'EXPORT_JOB', ej.id, ej.status, NULL::text, ej.row_count::bigint, ej.updated_at
            FROM export_jobs ej
            UNION ALL
            SELECT ea.workspace_id, 'export_artifact', 'EXPORT_ARTIFACT', ea.id, NULL::uuid, NULL::uuid,
                   NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, ea.storage_key,
                   'EXPORT_JOB', ea.export_job_id, 'STORED', NULL::text, ea.byte_size::bigint, ea.created_at
            FROM export_artifacts ea
            """;

    private final JdbcTemplate jdbcTemplate;

    public InvestigationStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
        data.put("currencyCode", rs.getString("currency_code"));
        data.put("amountMinor", nullableLong(rs, "amount_minor"));
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
                rs.getString("currency_code"),
                nullableLong(rs, "amount_minor"),
                occurredAt,
                data);
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
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
