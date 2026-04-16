package com.kayledger.api.shared.idempotency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class IdempotencyStore {

    private static final RowMapper<IdempotencyRecord> ROW_MAPPER = new RowMapper<>() {
        @Override
        public IdempotencyRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new IdempotencyRecord(
                    rs.getObject("id", UUID.class),
                    rs.getString("scope_kind"),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("actor_id", UUID.class),
                    rs.getString("route_key"),
                    rs.getString("idempotency_key"),
                    rs.getString("request_hash"),
                    rs.getString("status"),
                    (Integer) rs.getObject("response_status_code"),
                    rs.getString("response_body"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createIfAbsent(
            String scopeKind,
            UUID workspaceId,
            UUID actorId,
            String routeKey,
            String idempotencyKey,
            String requestHash) {
        jdbcTemplate.update("""
                INSERT INTO idempotency_records (
                    scope_kind,
                    workspace_id,
                    actor_id,
                    route_key,
                    idempotency_key,
                    request_hash
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """, scopeKind, workspaceId, actorId, routeKey, idempotencyKey, requestHash);
    }

    public Optional<IdempotencyRecord> findForUpdate(
            String scopeKind,
            UUID workspaceId,
            UUID actorId,
            String routeKey,
            String idempotencyKey) {
        return jdbcTemplate.query("""
                SELECT *
                FROM idempotency_records
                WHERE scope_kind = ?
                  AND COALESCE(workspace_id, '00000000-0000-0000-0000-000000000000'::uuid) = COALESCE(?::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
                  AND COALESCE(actor_id, '00000000-0000-0000-0000-000000000000'::uuid) = COALESCE(?::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
                  AND route_key = ?
                  AND idempotency_key = ?
                FOR UPDATE
                """, ROW_MAPPER, scopeKind, workspaceId, actorId, routeKey, idempotencyKey).stream().findFirst();
    }

    public void complete(UUID id, int responseStatusCode, String responseBody) {
        jdbcTemplate.update("""
                UPDATE idempotency_records
                SET status = 'COMPLETED',
                    response_status_code = ?,
                    response_body = CAST(? AS jsonb)
                WHERE id = ?
                """, responseStatusCode, responseBody, id);
    }

    public void fail(UUID id) {
        jdbcTemplate.update("""
                UPDATE idempotency_records
                SET status = 'FAILED'
                WHERE id = ?
                """, id);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
