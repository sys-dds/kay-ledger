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
import com.kayledger.api.evidence.model.FinanceEvidenceArtifact;
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
            rs.getObject("artifact_id", UUID.class),
            rs.getInt("export_version"),
            rs.getString("export_status"),
            rs.getString("artifact_format"),
            rs.getString("artifact_reference"),
            rs.getLong("artifact_size_bytes"),
            rs.getString("checksum_algorithm"),
            rs.getString("checksum_value"),
            rs.getObject("generated_by_actor_id", UUID.class),
            instant(rs, "generated_at"),
            instant(rs, "created_at"));

    private static final RowMapper<FinanceEvidenceArtifact> ARTIFACT_MAPPER = (rs, rowNum) -> new FinanceEvidenceArtifact(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("evidence_pack_id", UUID.class),
            rs.getString("artifact_format"),
            rs.getString("artifact_body"),
            rs.getLong("artifact_size_bytes"),
            rs.getString("checksum_algorithm"),
            rs.getString("checksum_value"),
            rs.getObject("generated_by_actor_id", UUID.class),
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

    public FinanceEvidenceArtifact createArtifact(UUID workspaceId, UUID packId, String format, String artifactBody, long sizeBytes, String checksumAlgorithm, String checksumValue, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO finance_evidence_export_artifacts (
                    workspace_id, evidence_pack_id, artifact_format, artifact_body, artifact_size_bytes,
                    checksum_algorithm, checksum_value, generated_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, ARTIFACT_MAPPER, workspaceId, packId, format, artifactBody, sizeBytes, checksumAlgorithm, checksumValue, actorId);
    }

    public FinanceEvidenceExport createExport(UUID workspaceId, UUID packId, UUID artifactId, String format, String artifactReference, long sizeBytes, String checksumAlgorithm, String checksumValue, UUID actorId) {
        int exportVersion = jdbcTemplate.queryForObject("""
                SELECT COALESCE(MAX(export_version), 0) + 1
                FROM finance_evidence_exports
                WHERE workspace_id = ?
                  AND evidence_pack_id = ?
                  AND artifact_format = ?
                """, Integer.class, workspaceId, packId, format);
        FinanceEvidenceExport export = jdbcTemplate.queryForObject("""
                INSERT INTO finance_evidence_exports (
                    workspace_id, evidence_pack_id, artifact_id, export_version, artifact_format, artifact_reference,
                    artifact_size_bytes, checksum_algorithm, checksum_value, generated_by_actor_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, EXPORT_MAPPER, workspaceId, packId, artifactId, exportVersion, format, artifactReference, sizeBytes, checksumAlgorithm, checksumValue, actorId);
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

    public Optional<FinanceEvidenceArtifact> artifactForExport(UUID workspaceId, UUID exportId) {
        return jdbcTemplate.query("""
                SELECT artifact.*
                FROM finance_evidence_exports export
                JOIN finance_evidence_export_artifacts artifact
                  ON artifact.workspace_id = export.workspace_id
                 AND artifact.id = export.artifact_id
                WHERE export.workspace_id = ?
                  AND export.id = ?
                """, ARTIFACT_MAPPER, workspaceId, exportId).stream().findFirst();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
