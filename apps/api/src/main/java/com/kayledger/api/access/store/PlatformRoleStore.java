package com.kayledger.api.access.store;

import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PlatformRoleStore {

    private final JdbcTemplate jdbcTemplate;

    public PlatformRoleStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void create(UUID actorId, String role) {
        jdbcTemplate.update("""
                INSERT INTO actor_platform_roles (actor_id, role)
                VALUES (?, ?)
                ON CONFLICT (actor_id, role) DO NOTHING
                """, actorId, role);
    }

    public List<String> listActiveRoles(UUID actorId) {
        return jdbcTemplate.query("""
                SELECT role
                FROM actor_platform_roles
                WHERE actor_id = ?
                  AND status = 'ACTIVE'
                ORDER BY role
                """, (rs, rowNum) -> rs.getString("role"), actorId);
    }
}
