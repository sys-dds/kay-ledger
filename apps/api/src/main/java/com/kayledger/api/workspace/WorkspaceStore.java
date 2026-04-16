package com.kayledger.api.workspace;

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
public class WorkspaceStore {

    private static final RowMapper<Workspace> WORKSPACE_ROW_MAPPER = new RowMapper<>() {
        @Override
        public Workspace mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Workspace(
                    rs.getObject("id", UUID.class),
                    rs.getString("slug"),
                    rs.getString("display_name"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public WorkspaceStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Workspace create(String slug, String displayName) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO workspaces (slug, display_name)
                VALUES (?, ?)
                RETURNING *
                """, WORKSPACE_ROW_MAPPER, slug, displayName);
    }

    public List<Workspace> list() {
        return jdbcTemplate.query("""
                SELECT *
                FROM workspaces
                ORDER BY created_at, slug
                """, WORKSPACE_ROW_MAPPER);
    }

    public Optional<Workspace> findBySlug(String slug) {
        return jdbcTemplate.query("""
                SELECT *
                FROM workspaces
                WHERE slug = ?
                """, WORKSPACE_ROW_MAPPER, slug).stream().findFirst();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
