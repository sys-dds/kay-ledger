package com.kayledger.api.evidence.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.evidence.model.FinanceEvidenceExport;
import com.kayledger.api.evidence.model.FinanceEvidencePack;
import com.kayledger.api.evidence.model.FinanceEvidencePackItem;

@Repository
public class FinanceEvidenceStore {

    private static final RowMapper<FinanceEvidencePack> PACK_MAPPER = (rs, rowNum) -> new FinanceEvidencePack(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("evidence_pack_type"),
            rs.getString("status"),
            rs.getObject("accounting_period_id", UUID.class),
            rs.getObject("finalized_statement_id", UUID.class),
            rs.getObject("reconciliation_run_id", UUID.class),
            rs.getObject("provider_truth_import_id", UUID.class),
            rs.getObject("approval_request_id", UUID.class),
            rs.getString("source_reference"),
            rs.getObject("generated_by_actor_id", UUID.class),
            rs.getString("snapshot_json"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FinanceEvidencePackItem> ITEM_MAPPER = (rs, rowNum) -> new FinanceEvidencePackItem(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("evidence_pack_id", UUID.class),
            rs.getString("source_reference_type"),
            rs.getObject("source_reference_id", UUID.class),
            rs.getString("source_reference_label"),
            rs.getString("item_snapshot_json"),
            instant(rs, "created_at"));

    private static final RowMapper<FinanceEvidenceExport> EXPORT_MAPPER = (rs, rowNum) -> new FinanceEvidenceExport(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("evidence_pack_id", UUID.class),
            rs.getString("export_status"),
            rs.getString("artifact_format"),
            rs.getString("artifact_reference"),
            rs.getLong("artifact_size_bytes"),
            rs.getString("checksum_algorithm"),
            rs.getString("checksum_value"),
            rs.getObject("generated_by_actor_id", UUID.class),
            instant(rs, "generated_at"),
            instant(rs, "created_at"));

    private final JdbcTemplate jdbcTemplate;

    public FinanceEvidenceStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public FinanceEvidencePack createPack(UUID workspaceId, String packType, UUID periodId, UUID statementId, UUID runId, UUID truthImportId, UUID approvalRequestId, String sourceReference, UUID actorId, String snapshotJson) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO finance_evidence_packs (
                    workspace_id, evidence_pack_type, accounting_period_id, finalized_statement_id,
                    reconciliation_run_id, provider_truth_import_id, approval_request_id,
                    source_reference, generated_by_actor_id, snapshot_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                RETURNING *, snapshot_json::text
                """, PACK_MAPPER, workspaceId, packType, periodId, statementId, runId, truthImportId, approvalRequestId, sourceReference, actorId, snapshotJson);
    }

    public FinanceEvidencePackItem createItem(UUID workspaceId, UUID packId, String sourceType, UUID sourceId, String label, String snapshotJson) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO finance_evidence_pack_items (
                    workspace_id, evidence_pack_id, source_reference_type, source_reference_id, source_reference_label, item_snapshot_json
                )
                VALUES (?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (workspace_id, evidence_pack_id, source_reference_type, source_reference_id) DO UPDATE
                SET source_reference_label = finance_evidence_pack_items.source_reference_label
                RETURNING *, item_snapshot_json::text
                """, ITEM_MAPPER, workspaceId, packId, sourceType, sourceId, label, snapshotJson);
    }

    public List<FinanceEvidencePack> listPacks(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *, snapshot_json::text
                FROM finance_evidence_packs
                WHERE workspace_id = ?
                ORDER BY created_at DESC, id
                """, PACK_MAPPER, workspaceId);
    }

    public List<FinanceEvidencePack> listPacksBySource(UUID workspaceId, String sourceReference) {
        return jdbcTemplate.query("""
                SELECT *, snapshot_json::text
                FROM finance_evidence_packs
                WHERE workspace_id = ?
                  AND source_reference = ?
                ORDER BY created_at DESC, id
                """, PACK_MAPPER, workspaceId, sourceReference);
    }

    public Optional<FinanceEvidencePack> findPack(UUID workspaceId, UUID packId) {
        return jdbcTemplate.query("""
                SELECT *, snapshot_json::text
                FROM finance_evidence_packs
                WHERE workspace_id = ?
                  AND id = ?
                """, PACK_MAPPER, workspaceId, packId).stream().findFirst();
    }

    public List<FinanceEvidencePackItem> listItems(UUID workspaceId, UUID packId) {
        return jdbcTemplate.query("""
                SELECT *, item_snapshot_json::text
                FROM finance_evidence_pack_items
                WHERE workspace_id = ?
                  AND evidence_pack_id = ?
                ORDER BY created_at, id
                """, ITEM_MAPPER, workspaceId, packId);
    }

    public FinanceEvidenceExport createExport(UUID workspaceId, UUID packId, String format, String artifactReference, long sizeBytes, String checksumAlgorithm, String checksumValue, UUID actorId) {
        FinanceEvidenceExport export = jdbcTemplate.queryForObject("""
                INSERT INTO finance_evidence_exports (
                    workspace_id, evidence_pack_id, artifact_format, artifact_reference, artifact_size_bytes,
                    checksum_algorithm, checksum_value, generated_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, evidence_pack_id, artifact_format) DO UPDATE
                SET artifact_reference = finance_evidence_exports.artifact_reference
                RETURNING *
                """, EXPORT_MAPPER, workspaceId, packId, format, artifactReference, sizeBytes, checksumAlgorithm, checksumValue, actorId);
        jdbcTemplate.update("""
                UPDATE finance_evidence_packs
                SET status = 'EXPORTED'
                WHERE workspace_id = ?
                  AND id = ?
                """, workspaceId, packId);
        return export;
    }

    public List<FinanceEvidenceExport> listExports(UUID workspaceId, UUID packId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM finance_evidence_exports
                WHERE workspace_id = ?
                  AND evidence_pack_id = ?
                ORDER BY generated_at DESC, id
                """, EXPORT_MAPPER, workspaceId, packId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
