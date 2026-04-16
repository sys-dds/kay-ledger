package com.kayledger.api.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
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
                    rs.getString("offer_type"),
                    rs.getString("pricing_metadata"),
                    (Integer) rs.getObject("duration_minutes"),
                    (Integer) rs.getObject("min_notice_minutes"),
                    (Integer) rs.getObject("max_notice_days"),
                    (Integer) rs.getObject("slot_interval_minutes"),
                    (Integer) rs.getObject("quantity_available"),
                    rs.getString("scheduling_metadata"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private static final RowMapper<OfferingPricingRule> PRICING_RULE_ROW_MAPPER = new RowMapper<>() {
        @Override
        public OfferingPricingRule mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OfferingPricingRule(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("offering_id", UUID.class),
                    rs.getString("rule_type"),
                    rs.getString("currency_code"),
                    rs.getLong("amount_minor"),
                    rs.getString("unit_name"),
                    rs.getInt("sort_order"),
                    rs.getString("status"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private static final RowMapper<OfferingAvailabilityWindow> AVAILABILITY_WINDOW_ROW_MAPPER = new RowMapper<>() {
        @Override
        public OfferingAvailabilityWindow mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OfferingAvailabilityWindow(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("offering_id", UUID.class),
                    rs.getInt("weekday"),
                    rs.getObject("start_local_time", LocalTime.class),
                    rs.getObject("end_local_time", LocalTime.class),
                    rs.getObject("effective_from", LocalDate.class),
                    rs.getObject("effective_to", LocalDate.class),
                    rs.getString("status"),
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
        return createDraft(
                workspaceId,
                providerProfileId,
                title,
                "SCHEDULED_TIME",
                pricingMetadata,
                durationMinutes == null ? 60 : durationMinutes,
                0,
                null,
                durationMinutes == null ? 60 : durationMinutes,
                null,
                schedulingMetadata);
    }

    public Offering createDraft(
            UUID workspaceId,
            UUID providerProfileId,
            String title,
            String offerType,
            String pricingMetadata,
            Integer durationMinutes,
            Integer minNoticeMinutes,
            Integer maxNoticeDays,
            Integer slotIntervalMinutes,
            Integer quantityAvailable,
            String schedulingMetadata) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO offerings (
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    offer_type,
                    pricing_metadata,
                    duration_minutes,
                    min_notice_minutes,
                    max_notice_days,
                    slot_interval_minutes,
                    quantity_available,
                    scheduling_metadata
                )
                VALUES (?, ?, ?, 'DRAFT', ?, CAST(? AS jsonb), ?, ?, ?, ?, ?, CAST(? AS jsonb))
                RETURNING
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    offer_type,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    min_notice_minutes,
                    max_notice_days,
                    slot_interval_minutes,
                    quantity_available,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                """, OFFERING_ROW_MAPPER,
                workspaceId,
                providerProfileId,
                title,
                offerType,
                pricingMetadata,
                durationMinutes,
                minNoticeMinutes,
                maxNoticeDays,
                slotIntervalMinutes,
                quantityAvailable,
                schedulingMetadata);
    }

    public List<Offering> listForWorkspace(UUID workspaceId) {
        return listOfferingsForWorkspace(workspaceId);
    }

    public List<OfferingDetails> listDetailsForWorkspace(UUID workspaceId) {
        return listOfferingsForWorkspace(workspaceId).stream()
                .map(offering -> new OfferingDetails(
                        offering,
                        listPricingRules(workspaceId, offering.id()),
                        listAvailabilityWindows(workspaceId, offering.id())))
                .toList();
    }

    public List<Offering> listOfferingsForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    offer_type,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    min_notice_minutes,
                    max_notice_days,
                    slot_interval_minutes,
                    quantity_available,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                FROM offerings
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, OFFERING_ROW_MAPPER, workspaceId);
    }

    public Optional<Offering> findForWorkspace(UUID workspaceId, UUID offeringId) {
        return jdbcTemplate.query("""
                SELECT
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    offer_type,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    min_notice_minutes,
                    max_notice_days,
                    slot_interval_minutes,
                    quantity_available,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                FROM offerings
                WHERE workspace_id = ?
                  AND id = ?
                """, OFFERING_ROW_MAPPER, workspaceId, offeringId).stream().findFirst();
    }

    public Offering publish(UUID workspaceId, UUID offeringId) {
        return changeStatus(workspaceId, offeringId, "PUBLISHED");
    }

    public Offering archive(UUID workspaceId, UUID offeringId) {
        return changeStatus(workspaceId, offeringId, "ARCHIVED");
    }

    public OfferingPricingRule createPricingRule(
            UUID workspaceId,
            UUID offeringId,
            String ruleType,
            String currencyCode,
            long amountMinor,
            String unitName,
            int sortOrder) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO offering_pricing_rules (
                    workspace_id,
                    offering_id,
                    rule_type,
                    currency_code,
                    amount_minor,
                    unit_name,
                    sort_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, PRICING_RULE_ROW_MAPPER, workspaceId, offeringId, ruleType, currencyCode, amountMinor, unitName, sortOrder);
    }

    public OfferingAvailabilityWindow createAvailabilityWindow(
            UUID workspaceId,
            UUID offeringId,
            int weekday,
            LocalTime startLocalTime,
            LocalTime endLocalTime,
            LocalDate effectiveFrom,
            LocalDate effectiveTo) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO offering_availability_windows (
                    workspace_id,
                    offering_id,
                    weekday,
                    start_local_time,
                    end_local_time,
                    effective_from,
                    effective_to
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, AVAILABILITY_WINDOW_ROW_MAPPER, workspaceId, offeringId, weekday, startLocalTime, endLocalTime, effectiveFrom, effectiveTo);
    }

    public List<OfferingPricingRule> listPricingRules(UUID workspaceId, UUID offeringId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM offering_pricing_rules
                WHERE workspace_id = ?
                  AND offering_id = ?
                  AND status = 'ACTIVE'
                ORDER BY sort_order, id
                """, PRICING_RULE_ROW_MAPPER, workspaceId, offeringId);
    }

    public List<OfferingAvailabilityWindow> listAvailabilityWindows(UUID workspaceId, UUID offeringId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM offering_availability_windows
                WHERE workspace_id = ?
                  AND offering_id = ?
                  AND status = 'ACTIVE'
                ORDER BY weekday, start_local_time, id
                """, AVAILABILITY_WINDOW_ROW_MAPPER, workspaceId, offeringId);
    }

    private Offering changeStatus(UUID workspaceId, UUID offeringId, String status) {
        return jdbcTemplate.queryForObject("""
                UPDATE offerings
                SET status = ?
                WHERE workspace_id = ?
                  AND id = ?
                RETURNING
                    id,
                    workspace_id,
                    provider_profile_id,
                    title,
                    status,
                    offer_type,
                    pricing_metadata::text AS pricing_metadata,
                    duration_minutes,
                    min_notice_minutes,
                    max_notice_days,
                    slot_interval_minutes,
                    quantity_available,
                    scheduling_metadata::text AS scheduling_metadata,
                    created_at,
                    updated_at
                """, OFFERING_ROW_MAPPER, status, workspaceId, offeringId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
