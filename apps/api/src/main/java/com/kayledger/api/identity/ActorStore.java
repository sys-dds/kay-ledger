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
public class ActorStore {

    private static final RowMapper<Actor> ACTOR_ROW_MAPPER = new RowMapper<>() {
        @Override
        public Actor mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Actor(
                    rs.getObject("id", UUID.class),
                    rs.getString("actor_key"),
                    rs.getString("display_name"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public ActorStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Actor create(String actorKey, String displayName) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO actors (actor_key, display_name)
                VALUES (?, ?)
                RETURNING *
                """, ACTOR_ROW_MAPPER, actorKey, displayName);
    }

    public List<Actor> list() {
        return jdbcTemplate.query("""
                SELECT *
                FROM actors
                ORDER BY created_at, actor_key
                """, ACTOR_ROW_MAPPER);
    }

    public List<Actor> listForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT a.*
                FROM actors a
                JOIN workspace_memberships wm ON wm.actor_id = a.id
                WHERE wm.workspace_id = ?
                  AND wm.status = 'ACTIVE'
                ORDER BY a.created_at, a.actor_key
                """, ACTOR_ROW_MAPPER, workspaceId);
    }

    public Optional<Actor> findByActorKey(String actorKey) {
        return jdbcTemplate.query("""
                SELECT *
                FROM actors
                WHERE actor_key = ?
                """, ACTOR_ROW_MAPPER, actorKey).stream().findFirst();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
