package com.kayledger.api.access.store;

import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MembershipScopeStore {

    private final JdbcTemplate jdbcTemplate;

    public MembershipScopeStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void create(UUID membershipId, String scope) {
        jdbcTemplate.update("""
                INSERT INTO workspace_membership_scopes (membership_id, scope)
                VALUES (?, ?)
                ON CONFLICT (membership_id, scope) DO NOTHING
                """, membershipId, scope);
    }

    public List<String> listForMembership(UUID membershipId) {
        return jdbcTemplate.query("""
                SELECT scope
                FROM workspace_membership_scopes
                WHERE membership_id = ?
                ORDER BY scope
                """, (rs, rowNum) -> rs.getString("scope"), membershipId);
    }
}
