package com.kayledger.api.payment.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.kayledger.api.payment.model.PaymentAttempt;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.ProviderPayableBalance;

@Repository
public class PaymentStore {

    private static final RowMapper<PaymentIntent> INTENT_MAPPER = (rs, rowNum) -> new PaymentIntent(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("booking_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("status"),
            rs.getString("currency_code"),
            rs.getLong("gross_amount_minor"),
            rs.getLong("fee_amount_minor"),
            rs.getLong("net_amount_minor"),
            rs.getLong("authorized_amount_minor"),
            rs.getLong("captured_amount_minor"),
            rs.getLong("settled_amount_minor"),
            rs.getString("external_reference"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<PaymentAttempt> ATTEMPT_MAPPER = (rs, rowNum) -> new PaymentAttempt(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("payment_intent_id", UUID.class),
            rs.getString("attempt_type"),
            rs.getString("status"),
            rs.getLong("amount_minor"),
            rs.getString("external_reference"),
            rs.getObject("journal_entry_id", UUID.class),
            instant(rs, "created_at"));

    private static final RowMapper<ProviderPayableBalance> PAYABLE_MAPPER = (rs, rowNum) -> new ProviderPayableBalance(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getLong("payable_amount_minor"),
            rs.getString("source"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private final JdbcTemplate jdbcTemplate;

    public PaymentStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public PaymentIntent createIntent(
            UUID workspaceId,
            UUID bookingId,
            UUID providerProfileId,
            String currencyCode,
            long grossAmountMinor,
            long feeAmountMinor,
            long netAmountMinor,
            String externalReference) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO payment_intents (
                    workspace_id,
                    booking_id,
                    provider_profile_id,
                    currency_code,
                    gross_amount_minor,
                    fee_amount_minor,
                    net_amount_minor,
                    external_reference
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workspace_id, booking_id) DO UPDATE
                SET external_reference = COALESCE(payment_intents.external_reference, EXCLUDED.external_reference)
                RETURNING *
                """, INTENT_MAPPER, workspaceId, bookingId, providerProfileId, currencyCode, grossAmountMinor, feeAmountMinor, netAmountMinor, externalReference);
    }

    public Optional<PaymentIntent> find(UUID workspaceId, UUID paymentIntentId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payment_intents
                WHERE workspace_id = ?
                  AND id = ?
                """, INTENT_MAPPER, workspaceId, paymentIntentId).stream().findFirst();
    }

    public Optional<PaymentIntent> findByBooking(UUID workspaceId, UUID bookingId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payment_intents
                WHERE workspace_id = ?
                  AND booking_id = ?
                """, INTENT_MAPPER, workspaceId, bookingId).stream().findFirst();
    }

    public List<PaymentIntent> list(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payment_intents
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, INTENT_MAPPER, workspaceId);
    }

    public PaymentIntent authorize(UUID workspaceId, UUID paymentIntentId, long amountMinor) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'AUTHORIZED',
                    authorized_amount_minor = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'CREATED'
                  AND ? <= gross_amount_minor
                RETURNING *
                """, INTENT_MAPPER, amountMinor, workspaceId, paymentIntentId, amountMinor);
    }

    public PaymentIntent capture(UUID workspaceId, UUID paymentIntentId, long amountMinor) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'CAPTURED',
                    captured_amount_minor = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'AUTHORIZED'
                  AND ? <= authorized_amount_minor
                RETURNING *
                """, INTENT_MAPPER, amountMinor, workspaceId, paymentIntentId, amountMinor);
    }

    public PaymentIntent settle(UUID workspaceId, UUID paymentIntentId, long amountMinor) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'SETTLED',
                    settled_amount_minor = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'CAPTURED'
                  AND ? <= captured_amount_minor
                RETURNING *
                """, INTENT_MAPPER, amountMinor, workspaceId, paymentIntentId, amountMinor);
    }

    public PaymentIntent cancel(UUID workspaceId, UUID paymentIntentId) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'CANCELLED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('CREATED', 'REQUIRES_ACTION', 'AUTHORIZED')
                RETURNING *
                """, INTENT_MAPPER, workspaceId, paymentIntentId);
    }

    public PaymentIntent requireAction(UUID workspaceId, UUID paymentIntentId) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'REQUIRES_ACTION'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'CREATED'
                RETURNING *
                """, INTENT_MAPPER, workspaceId, paymentIntentId);
    }

    public PaymentIntent fail(UUID workspaceId, UUID paymentIntentId) {
        return jdbcTemplate.queryForObject("""
                UPDATE payment_intents
                SET status = 'FAILED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('CREATED', 'REQUIRES_ACTION')
                RETURNING *
                """, INTENT_MAPPER, workspaceId, paymentIntentId);
    }

    public PaymentAttempt createAttempt(
            UUID workspaceId,
            UUID paymentIntentId,
            String attemptType,
            String status,
            long amountMinor,
            String externalReference,
            UUID journalEntryId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO payment_attempts (
                    workspace_id,
                    payment_intent_id,
                    attempt_type,
                    status,
                    amount_minor,
                    external_reference,
                    journal_entry_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, ATTEMPT_MAPPER, workspaceId, paymentIntentId, attemptType, status, amountMinor, externalReference, journalEntryId);
    }

    public void attachAttemptJournal(UUID workspaceId, UUID attemptId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE payment_attempts
                SET journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND journal_entry_id IS NULL
                """, journalEntryId, workspaceId, attemptId);
    }

    public List<PaymentAttempt> listAttempts(UUID workspaceId, UUID paymentIntentId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payment_attempts
                WHERE workspace_id = ?
                  AND payment_intent_id = ?
                ORDER BY created_at, id
                """, ATTEMPT_MAPPER, workspaceId, paymentIntentId);
    }

    public ProviderPayableBalance refreshPayableBalance(UUID workspaceId, UUID providerProfileId, String currencyCode) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO provider_payable_balances (
                    workspace_id,
                    provider_profile_id,
                    currency_code,
                    payable_amount_minor
                )
                SELECT ?, ?, ?, COALESCE(SUM(net_amount_minor), 0)
                FROM payment_intents
                WHERE workspace_id = ?
                  AND provider_profile_id = ?
                  AND currency_code = ?
                  AND status = 'SETTLED'
                ON CONFLICT (workspace_id, provider_profile_id, currency_code) DO UPDATE
                SET payable_amount_minor = EXCLUDED.payable_amount_minor
                RETURNING *
                """, PAYABLE_MAPPER, workspaceId, providerProfileId, currencyCode, workspaceId, providerProfileId, currencyCode);
    }

    public List<ProviderPayableBalance> listPayableBalances(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_payable_balances
                WHERE workspace_id = ?
                ORDER BY provider_profile_id, currency_code
                """, PAYABLE_MAPPER, workspaceId);
    }

    public List<ProviderPayableBalance> listPayableBalancesForProvider(UUID workspaceId, UUID providerProfileId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM provider_payable_balances
                WHERE workspace_id = ?
                  AND provider_profile_id = ?
                ORDER BY currency_code
                """, PAYABLE_MAPPER, workspaceId, providerProfileId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
