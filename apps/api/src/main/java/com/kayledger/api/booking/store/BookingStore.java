package com.kayledger.api.booking.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.booking.model.Booking;
import com.kayledger.api.booking.model.BookingHold;
import com.kayledger.api.shared.api.BadRequestException;

@Repository
public class BookingStore {

    private static final RowMapper<Booking> BOOKING_ROW_MAPPER = new RowMapper<>() {
        @Override
        public Booking mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Booking(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("offering_id", UUID.class),
                    rs.getObject("provider_profile_id", UUID.class),
                    rs.getObject("customer_profile_id", UUID.class),
                    rs.getString("offer_type"),
                    rs.getString("status"),
                    instantOrNull(rs, "scheduled_start_at"),
                    instantOrNull(rs, "scheduled_end_at"),
                    rs.getInt("quantity_reserved"),
                    instant(rs, "hold_expires_at"),
                    instantOrNull(rs, "confirmed_at"),
                    instantOrNull(rs, "cancelled_at"),
                    rs.getString("currency_code"),
                    (Long) rs.getObject("gross_amount_minor"),
                    (Long) rs.getObject("fee_amount_minor"),
                    (Long) rs.getObject("net_amount_minor"),
                    rs.getObject("financial_reference_id", UUID.class),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private static final RowMapper<BookingHold> HOLD_ROW_MAPPER = new RowMapper<>() {
        @Override
        public BookingHold mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new BookingHold(
                    rs.getObject("id", UUID.class),
                    rs.getObject("workspace_id", UUID.class),
                    rs.getObject("booking_id", UUID.class),
                    rs.getString("status"),
                    instant(rs, "expires_at"),
                    rs.getString("release_reason"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"));
        }
    };

    private final JdbcTemplate jdbcTemplate;

    public BookingStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Booking createHeld(
            UUID workspaceId,
            UUID offeringId,
            UUID providerProfileId,
            UUID customerProfileId,
            String offerType,
            Instant scheduledStartAt,
            Instant scheduledEndAt,
            int quantityReserved,
            Instant holdExpiresAt,
            String currencyCode,
            long grossAmountMinor,
            long feeAmountMinor,
            long netAmountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO bookings (
                    workspace_id,
                    offering_id,
                    provider_profile_id,
                    customer_profile_id,
                    offer_type,
                    status,
                    scheduled_start_at,
                    scheduled_end_at,
                    quantity_reserved,
                    hold_expires_at,
                    currency_code,
                    gross_amount_minor,
                    fee_amount_minor,
                    net_amount_minor
                )
                VALUES (?, ?, ?, ?, ?, 'HELD', ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, BOOKING_ROW_MAPPER,
                workspaceId,
                offeringId,
                providerProfileId,
                customerProfileId,
                offerType,
                timestampOrNull(scheduledStartAt),
                timestampOrNull(scheduledEndAt),
                quantityReserved,
                timestamp(holdExpiresAt),
                currencyCode,
                grossAmountMinor,
                feeAmountMinor,
                netAmountMinor);
    }

    public void attachFinancialReference(UUID workspaceId, UUID bookingId, UUID journalEntryId) {
        int updated = jdbcTemplate.update("""
                UPDATE bookings
                SET financial_reference_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND EXISTS (
                      SELECT 1
                      FROM journal_entries journal
                      WHERE journal.id = ?
                        AND journal.workspace_id = bookings.workspace_id
                  )
                """, journalEntryId, workspaceId, bookingId, journalEntryId);
        if (updated != 1) {
            throw new BadRequestException("Financial reference is not valid for this booking workspace.");
        }
    }

    public BookingHold createHold(UUID workspaceId, UUID bookingId, Instant expiresAt) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO booking_holds (workspace_id, booking_id, status, expires_at)
                VALUES (?, ?, 'HELD', ?)
                RETURNING *
                """, HOLD_ROW_MAPPER, workspaceId, bookingId, timestamp(expiresAt));
    }

    public Optional<Booking> find(UUID workspaceId, UUID bookingId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM bookings
                WHERE workspace_id = ?
                  AND id = ?
                """, BOOKING_ROW_MAPPER, workspaceId, bookingId).stream().findFirst();
    }

    public BookingHold holdForBooking(UUID workspaceId, UUID bookingId) {
        return jdbcTemplate.queryForObject("""
                SELECT *
                FROM booking_holds
                WHERE workspace_id = ?
                  AND booking_id = ?
                """, HOLD_ROW_MAPPER, workspaceId, bookingId);
    }

    public List<Booking> listForWorkspace(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM bookings
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, BOOKING_ROW_MAPPER, workspaceId);
    }

    public List<Booking> listForProvider(UUID workspaceId, List<UUID> providerProfileIds) {
        if (providerProfileIds.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query("""
                SELECT *
                FROM bookings
                WHERE workspace_id = ?
                  AND provider_profile_id = ANY (?)
                ORDER BY created_at, id
                """, BOOKING_ROW_MAPPER, workspaceId, providerProfileIds.toArray(UUID[]::new));
    }

    public List<Booking> listForCustomer(UUID workspaceId, List<UUID> customerProfileIds) {
        if (customerProfileIds.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query("""
                SELECT *
                FROM bookings
                WHERE workspace_id = ?
                  AND customer_profile_id = ANY (?)
                ORDER BY created_at, id
                """, BOOKING_ROW_MAPPER, workspaceId, customerProfileIds.toArray(UUID[]::new));
    }

    public int activeQuantityReserved(UUID workspaceId, UUID offeringId, Instant now) {
        Integer reserved = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(quantity_reserved), 0)
                FROM bookings
                WHERE workspace_id = ?
                  AND offering_id = ?
                  AND offer_type = 'QUANTITY'
                  AND (
                      status = 'CONFIRMED'
                      OR (status = 'HELD' AND hold_expires_at > ?)
                  )
                """, Integer.class, workspaceId, offeringId, timestamp(now));
        return reserved == null ? 0 : reserved;
    }

    public void expireHeldForOffering(UUID workspaceId, UUID offeringId, Instant now) {
        jdbcTemplate.update("""
                UPDATE booking_holds h
                SET status = 'EXPIRED',
                    release_reason = 'EXPIRED'
                FROM bookings b
                WHERE h.workspace_id = b.workspace_id
                  AND h.booking_id = b.id
                  AND b.workspace_id = ?
                  AND b.offering_id = ?
                  AND b.status = 'HELD'
                  AND b.hold_expires_at <= ?
                  AND h.status = 'HELD'
                """, workspaceId, offeringId, timestamp(now));
        jdbcTemplate.update("""
                UPDATE bookings
                SET status = 'EXPIRED'
                WHERE workspace_id = ?
                  AND offering_id = ?
                  AND status = 'HELD'
                  AND hold_expires_at <= ?
                """, workspaceId, offeringId, timestamp(now));
    }

    public Optional<Booking> cancelHeld(UUID workspaceId, UUID bookingId, Instant now) {
        try {
            Booking booking = jdbcTemplate.queryForObject("""
                    UPDATE bookings
                    SET status = 'CANCELLED',
                        cancelled_at = ?
                    WHERE workspace_id = ?
                      AND id = ?
                      AND status = 'HELD'
                    RETURNING *
                    """, BOOKING_ROW_MAPPER, timestamp(now), workspaceId, bookingId);
            jdbcTemplate.update("""
                    UPDATE booking_holds
                    SET status = 'CANCELLED',
                        release_reason = 'CANCELLED'
                    WHERE workspace_id = ?
                      AND booking_id = ?
                      AND status = 'HELD'
                    """, workspaceId, bookingId);
            return Optional.of(booking);
        } catch (EmptyResultDataAccessException exception) {
            return Optional.empty();
        }
    }

    public Optional<Booking> expireHeld(UUID workspaceId, UUID bookingId, Instant now) {
        try {
            Booking booking = jdbcTemplate.queryForObject("""
                    UPDATE bookings
                    SET status = 'EXPIRED'
                    WHERE workspace_id = ?
                      AND id = ?
                      AND status = 'HELD'
                      AND hold_expires_at <= ?
                    RETURNING *
                    """, BOOKING_ROW_MAPPER, workspaceId, bookingId, timestamp(now));
            jdbcTemplate.update("""
                    UPDATE booking_holds
                    SET status = 'EXPIRED',
                        release_reason = 'EXPIRED'
                    WHERE workspace_id = ?
                      AND booking_id = ?
                      AND status = 'HELD'
                    """, workspaceId, bookingId);
            return Optional.of(booking);
        } catch (EmptyResultDataAccessException exception) {
            return Optional.empty();
        }
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column) == null ? null : rs.getTimestamp(column).toInstant();
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.from(value);
    }

    private static Timestamp timestampOrNull(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }
}
