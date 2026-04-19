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

import com.kayledger.api.region.model.RegionChaosFault;

@Repository
public class RegionFaultStore {

    private static final RowMapper<RegionChaosFault> FAULT_MAPPER = (rs, rowNum) -> new RegionChaosFault(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getString("fault_type"),
            rs.getString("scope"),
            rs.getString("status"),
            rs.getString("parameters_json"),
            rs.getString("reason"),
            rs.getObject("created_by_actor_id", UUID.class),
            rs.getTimestamp("created_at").toInstant(),
            nullableInstant(rs, "cleared_at"));

    private final JdbcTemplate jdbcTemplate;

    public RegionFaultStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RegionChaosFault create(UUID workspaceId, String faultType, String scope, String parametersJson, String reason, UUID actorId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO region_chaos_faults (workspace_id, fault_type, scope, parameters_json, reason, created_by_actor_id)
                VALUES (?, ?, ?, ?::jsonb, ?, ?)
                RETURNING id, workspace_id, fault_type, scope, status, parameters_json::text, reason, created_by_actor_id, created_at, cleared_at
                """, FAULT_MAPPER, workspaceId, faultType, scope, parametersJson, reason, actorId);
    }

    public List<RegionChaosFault> active(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, fault_type, scope, status, parameters_json::text, reason, created_by_actor_id, created_at, cleared_at
                FROM region_chaos_faults
                WHERE status = 'ACTIVE'
                  AND (workspace_id = ? OR scope = 'REGION')
                ORDER BY created_at DESC
                """, FAULT_MAPPER, workspaceId);
    }

    public Optional<RegionChaosFault> activeFault(UUID workspaceId, String faultType) {
        return jdbcTemplate.query("""
                SELECT id, workspace_id, fault_type, scope, status, parameters_json::text, reason, created_by_actor_id, created_at, cleared_at
                FROM region_chaos_faults
                WHERE status = 'ACTIVE'
                  AND fault_type = ?
                  AND (workspace_id = ? OR scope = 'REGION')
                ORDER BY created_at DESC
                LIMIT 1
                """, FAULT_MAPPER, faultType, workspaceId).stream().findFirst();
    }

    public RegionChaosFault clear(UUID workspaceId, UUID faultId) {
        return jdbcTemplate.queryForObject("""
                UPDATE region_chaos_faults
                SET status = 'CLEARED',
                    cleared_at = now()
                WHERE id = ?
                  AND (workspace_id = ? OR scope = 'REGION')
                  AND status = 'ACTIVE'
                RETURNING id, workspace_id, fault_type, scope, status, parameters_json::text, reason, created_by_actor_id, created_at, cleared_at
                """, FAULT_MAPPER, faultId, workspaceId);
    }

    private static java.time.Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
