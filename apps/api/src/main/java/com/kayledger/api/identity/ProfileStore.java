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
public class ProfileStore {

    private static final RowMapper<ProviderProfile> PROVIDER_ROW_MAPPER = new RowMapper<>() {
        @Override
        public ProviderProfile mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new ProviderProfile(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("actor_id", UUID.class),
                    rs.getString("display_name"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private static final RowMapper<CustomerProfile> CUSTOMER_ROW_MAPPER = new RowMapper<>() {
        @Override
        public CustomerProfile mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new CustomerProfile(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("actor_id", UUID.class),
                    rs.getString("display_name"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public ProfileStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ProviderProfile createProvider(UUID workspaceId, UUID actorId, String displayName) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_profiles (workspace_id, actor_id, display_name)
                VALUES (?, ?, ?)
                RETURNING *
                """, PROVIDER_ROW_MAPPER, workspaceId, actorId, displayName);
    }

    public List<ProviderProfile> listProviders(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_profiles
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, PROVIDER_ROW_MAPPER, workspaceId);
    }

    public Optional<ProviderProfile> findProvider(UUID workspaceId, UUID providerProfileId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_profiles
                WHERE workspace_id = ?
                  AND id = ?
                """, PROVIDER_ROW_MAPPER, workspaceId, providerProfileId).stream().findFirst();
    }

    public CustomerProfile createCustomer(UUID workspaceId, UUID actorId, String displayName) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO customer_profiles (workspace_id, actor_id, display_name)
                VALUES (?, ?, ?)
                RETURNING *
                """, CUSTOMER_ROW_MAPPER, workspaceId, actorId, displayName);
    }

    public List<CustomerProfile> listCustomers(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM customer_profiles
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, CUSTOMER_ROW_MAPPER, workspaceId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
