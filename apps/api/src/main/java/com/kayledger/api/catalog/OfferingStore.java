package com.kayledger.api.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class OfferingStore {

    private static final RowMapper<Offering> OFFERING_ROW_MAPPER = new RowMapper<>() {
        @Override
        public Offering mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Offering(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("provider_profile_id", UUID.class),
                    rs.getString("title"),
                    rs.getString("status"),
                    rs.getString("pricing_metadata"),
                    (Integer) rs.getObject("duration_minutes"),
                    rs.getString("scheduling_metadata"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public OfferingStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Offering create(
            UUID workspaceId,
            UUID providerProfileId,
            String title,
            String pricingMetadata,
            Integer durationMinutes,
            String schedulingMetadata) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO offerings (
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    pricing_metadata,
                    duration_minutes,
                    scheduling_metadata
                )
                VALUES (?, ?, ?, 'ACTIVE', CAST(? AS jsonb), ?, CAST(? AS jsonb))
                RETURNING
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                """, OFFERING_ROW_MAPPER, workspaceId, providerProfileId, title, pricingMetadata, durationMinutes, schedulingMetadata);
    }

    public List<Offering> listForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                FROM offerings
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, OFFERING_ROW_MAPPER, workspaceId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
