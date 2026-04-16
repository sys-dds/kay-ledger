package com.kayledger.api.identity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class WorkspaceMembershipStore {

    private static final RowMapper<WorkspaceMembership> MEMBERSHIP_ROW_MAPPER = new RowMapper<>() {
        @Override
        public WorkspaceMembership mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new WorkspaceMembership(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("actor_id", UUID.class),
                    rs.getString("role"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public WorkspaceMembershipStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public WorkspaceMembership create(UUID workspaceId, UUID actorId, String role) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO workspace_memberships (workspace_id, actor_id, role)
                VALUES (?, ?, ?)
                RETURNING *
                """, MEMBERSHIP_ROW_MAPPER, workspaceId, actorId, role);
    }

    public List<WorkspaceMembership> listForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM workspace_memberships
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, MEMBERSHIP_ROW_MAPPER, workspaceId);
    }

    public int countForWorkspace(UUID workspaceId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM workspace_memberships
                WHERE workspace_id = ?
                """, Integer.class, workspaceId);
        return count == null ? 0 : count;
    }

    public Optional<WorkspaceMembership> findActive(UUID workspaceId, UUID actorId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM workspace_memberships
                WHERE workspace_id = ?
                  AND actor_id = ?
                  AND status = 'ACTIVE'
                """, MEMBERSHIP_ROW_MAPPER, workspaceId, actorId).stream().findFirst();
    }

    public Optional<WorkspaceMembership> findActiveByActor(UUID workspaceId, UUID actorId) {
        return findActive(workspaceId, actorId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
