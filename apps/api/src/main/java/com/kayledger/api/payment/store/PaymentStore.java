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
import com.kayledger.api.payment.model.DisputeRecord;
import com.kayledger.api.payment.model.FrozenFund;
import com.kayledger.api.payment.model.PayoutAttempt;
import com.kayledger.api.payment.model.PayoutRequest;
import com.kayledger.api.payment.model.ProviderBalanceSummary;
import com.kayledger.api.payment.model.ProviderPayableBalance;
import com.kayledger.api.payment.model.RefundAttempt;
import com.kayledger.api.payment.model.RefundRecord;

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

    private static final RowMapper<PayoutRequest> PAYOUT_MAPPER = (rs, rowNum) -> new PayoutRequest(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getLong("requested_amount_minor"),
            rs.getString("status"),
            rs.getString("failure_reason"),
            rs.getObject("journal_entry_id", UUID.class),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<PayoutAttempt> PAYOUT_ATTEMPT_MAPPER = (rs, rowNum) -> new PayoutAttempt(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("payout_request_id", UUID.class),
            rs.getInt("attempt_number"),
            rs.getString("status"),
            rs.getString("failure_reason"),
            rs.getString("external_reference"),
            rs.getObject("journal_entry_id", UUID.class),
            instant(rs, "created_at"));

    private static final RowMapper<RefundRecord> REFUND_MAPPER = (rs, rowNum) -> new RefundRecord(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("payment_intent_id", UUID.class),
            rs.getObject("booking_id", UUID.class),
            rs.getString("refund_type"),
            rs.getLong("amount_minor"),
            rs.getLong("payable_reduction_amount_minor"),
            rs.getString("status"),
            rs.getObject("journal_entry_id", UUID.class),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<RefundAttempt> REFUND_ATTEMPT_MAPPER = (rs, rowNum) -> new RefundAttempt(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("refund_id", UUID.class),
            rs.getString("status"),
            rs.getString("failure_reason"),
            rs.getString("external_reference"),
            instant(rs, "created_at"));

    private static final RowMapper<DisputeRecord> DISPUTE_MAPPER = (rs, rowNum) -> new DisputeRecord(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("payment_intent_id", UUID.class),
            rs.getObject("booking_id", UUID.class),
            rs.getLong("disputed_amount_minor"),
            rs.getLong("frozen_amount_minor"),
            rs.getString("status"),
            rs.getString("resolution"),
            rs.getObject("open_journal_entry_id", UUID.class),
            rs.getObject("resolve_journal_entry_id", UUID.class),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<FrozenFund> FROZEN_MAPPER = (rs, rowNum) -> new FrozenFund(
            rs.getObject("id", UUID.class),
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getObject("dispute_id", UUID.class),
            rs.getString("currency_code"),
            rs.getLong("amount_minor"),
            rs.getString("status"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"));

    private static final RowMapper<ProviderBalanceSummary> BALANCE_SUMMARY_MAPPER = (rs, rowNum) -> new ProviderBalanceSummary(
            rs.getObject("workspace_id", UUID.class),
            rs.getObject("provider_profile_id", UUID.class),
            rs.getString("currency_code"),
            rs.getLong("payable_amount_minor"),
            rs.getLong("pending_payout_amount_minor"),
            rs.getLong("paid_out_amount_minor"),
            rs.getLong("refunded_amount_minor"),
            rs.getLong("frozen_amount_minor"),
            rs.getLong("available_amount_minor"));

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

    public ProviderBalanceSummary balanceSummary(UUID workspaceId, UUID providerProfileId, String currencyCode) {
        return jdbcTemplate.queryForObject("""
                WITH settled AS (
                    SELECT COALESCE(SUM(net_amount_minor), 0) AS amount
                    FROM payment_intents
                    WHERE workspace_id = ?
                      AND provider_profile_id = ?
                      AND currency_code = ?
                      AND status = 'SETTLED'
                ),
                pending_payouts AS (
                    SELECT COALESCE(SUM(requested_amount_minor), 0) AS amount
                    FROM payout_requests
                    WHERE workspace_id = ?
                      AND provider_profile_id = ?
                      AND currency_code = ?
                      AND status IN ('REQUESTED', 'PROCESSING')
                ),
                paid_out AS (
                    SELECT COALESCE(SUM(requested_amount_minor), 0) AS amount
                    FROM payout_requests
                    WHERE workspace_id = ?
                      AND provider_profile_id = ?
                      AND currency_code = ?
                      AND status = 'SUCCEEDED'
                ),
                refunded AS (
                    SELECT COALESCE(SUM(r.payable_reduction_amount_minor), 0) AS amount
                    FROM refunds r
                    JOIN payment_intents pi ON pi.workspace_id = r.workspace_id
                     AND pi.id = r.payment_intent_id
                    WHERE r.workspace_id = ?
                      AND pi.provider_profile_id = ?
                      AND pi.currency_code = ?
                      AND r.status = 'SUCCEEDED'
                ),
                frozen AS (
                    SELECT COALESCE(SUM(amount_minor), 0) AS amount
                    FROM frozen_funds
                    WHERE workspace_id = ?
                      AND provider_profile_id = ?
                      AND currency_code = ?
                      AND status = 'FROZEN'
                ),
                consumed_disputes AS (
                    SELECT COALESCE(SUM(amount_minor), 0) AS amount
                    FROM frozen_funds
                    WHERE workspace_id = ?
                      AND provider_profile_id = ?
                      AND currency_code = ?
                      AND status = 'CONSUMED'
                )
                SELECT ?::uuid AS workspace_id,
                       ?::uuid AS provider_profile_id,
                       ? AS currency_code,
                       settled.amount AS payable_amount_minor,
                       pending_payouts.amount AS pending_payout_amount_minor,
                       paid_out.amount AS paid_out_amount_minor,
                       refunded.amount AS refunded_amount_minor,
                       frozen.amount AS frozen_amount_minor,
                       GREATEST(settled.amount - pending_payouts.amount - paid_out.amount - refunded.amount - frozen.amount - consumed_disputes.amount, 0) AS available_amount_minor
                FROM settled, pending_payouts, paid_out, refunded, frozen, consumed_disputes
                """, BALANCE_SUMMARY_MAPPER,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode,
                workspaceId, providerProfileId, currencyCode);
    }

    public List<ProviderBalanceSummary> listProviderBalanceSummaries(UUID workspaceId) {
        return jdbcTemplate.query("""
                WITH provider_currencies AS (
                    SELECT DISTINCT workspace_id, provider_profile_id, currency_code
                    FROM payment_intents
                    WHERE workspace_id = ?
                    UNION
                    SELECT DISTINCT workspace_id, provider_profile_id, currency_code
                    FROM payout_requests
                    WHERE workspace_id = ?
                    UNION
                    SELECT DISTINCT workspace_id, provider_profile_id, currency_code
                    FROM frozen_funds
                    WHERE workspace_id = ?
                )
                SELECT pc.workspace_id,
                       pc.provider_profile_id,
                       pc.currency_code,
                       COALESCE((SELECT SUM(net_amount_minor)
                           FROM payment_intents pi
                           WHERE pi.workspace_id = pc.workspace_id
                             AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code
                             AND pi.status = 'SETTLED'), 0) AS payable_amount_minor,
                       COALESCE((SELECT SUM(requested_amount_minor)
                           FROM payout_requests pr
                           WHERE pr.workspace_id = pc.workspace_id
                             AND pr.provider_profile_id = pc.provider_profile_id
                             AND pr.currency_code = pc.currency_code
                             AND pr.status IN ('REQUESTED', 'PROCESSING')), 0) AS pending_payout_amount_minor,
                       COALESCE((SELECT SUM(requested_amount_minor)
                           FROM payout_requests pr
                           WHERE pr.workspace_id = pc.workspace_id
                             AND pr.provider_profile_id = pc.provider_profile_id
                             AND pr.currency_code = pc.currency_code
                             AND pr.status = 'SUCCEEDED'), 0) AS paid_out_amount_minor,
                       COALESCE((SELECT SUM(r.payable_reduction_amount_minor)
                           FROM refunds r
                           JOIN payment_intents pi ON pi.workspace_id = r.workspace_id
                            AND pi.id = r.payment_intent_id
                           WHERE r.workspace_id = pc.workspace_id
                             AND pi.provider_profile_id = pc.provider_profile_id
                             AND pi.currency_code = pc.currency_code
                             AND r.status = 'SUCCEEDED'), 0) AS refunded_amount_minor,
                       COALESCE((SELECT SUM(amount_minor)
                           FROM frozen_funds ff
                           WHERE ff.workspace_id = pc.workspace_id
                             AND ff.provider_profile_id = pc.provider_profile_id
                             AND ff.currency_code = pc.currency_code
                             AND ff.status = 'FROZEN'), 0) AS frozen_amount_minor,
                       GREATEST(
                           COALESCE((SELECT SUM(net_amount_minor)
                               FROM payment_intents pi
                               WHERE pi.workspace_id = pc.workspace_id
                                 AND pi.provider_profile_id = pc.provider_profile_id
                                 AND pi.currency_code = pc.currency_code
                                 AND pi.status = 'SETTLED'), 0)
                           - COALESCE((SELECT SUM(requested_amount_minor)
                               FROM payout_requests pr
                               WHERE pr.workspace_id = pc.workspace_id
                                 AND pr.provider_profile_id = pc.provider_profile_id
                                 AND pr.currency_code = pc.currency_code
                                 AND pr.status IN ('REQUESTED', 'PROCESSING', 'SUCCEEDED')), 0)
                           - COALESCE((SELECT SUM(r.payable_reduction_amount_minor)
                               FROM refunds r
                               JOIN payment_intents pi ON pi.workspace_id = r.workspace_id
                                AND pi.id = r.payment_intent_id
                               WHERE r.workspace_id = pc.workspace_id
                                 AND pi.provider_profile_id = pc.provider_profile_id
                                 AND pi.currency_code = pc.currency_code
                                 AND r.status = 'SUCCEEDED'), 0)
                           - COALESCE((SELECT SUM(amount_minor)
                               FROM frozen_funds ff
                               WHERE ff.workspace_id = pc.workspace_id
                                 AND ff.provider_profile_id = pc.provider_profile_id
                                 AND ff.currency_code = pc.currency_code
                                 AND ff.status = 'FROZEN'), 0)
                           - COALESCE((SELECT SUM(amount_minor)
                               FROM frozen_funds ff
                               WHERE ff.workspace_id = pc.workspace_id
                                 AND ff.provider_profile_id = pc.provider_profile_id
                                 AND ff.currency_code = pc.currency_code
                                 AND ff.status = 'CONSUMED'), 0),
                           0) AS available_amount_minor
                FROM provider_currencies pc
                ORDER BY pc.provider_profile_id, pc.currency_code
                """, BALANCE_SUMMARY_MAPPER, workspaceId, workspaceId, workspaceId);
    }

    public PayoutRequest createPayoutRequest(UUID workspaceId, UUID providerProfileId, String currencyCode, long requestedAmountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO payout_requests (
                    workspace_id,
                    provider_profile_id,
                    currency_code,
                    requested_amount_minor
                )
                VALUES (?, ?, ?, ?)
                RETURNING *
                """, PAYOUT_MAPPER, workspaceId, providerProfileId, currencyCode, requestedAmountMinor);
    }

    public Optional<PayoutRequest> findPayout(UUID workspaceId, UUID payoutRequestId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payout_requests
                WHERE workspace_id = ?
                  AND id = ?
                """, PAYOUT_MAPPER, workspaceId, payoutRequestId).stream().findFirst();
    }

    public List<PayoutRequest> listPayouts(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payout_requests
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, PAYOUT_MAPPER, workspaceId);
    }

    public PayoutRequest markPayoutProcessing(UUID workspaceId, UUID payoutRequestId) {
        return jdbcTemplate.queryForObject("""
                UPDATE payout_requests
                SET status = 'PROCESSING',
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('REQUESTED', 'FAILED')
                RETURNING *
                """, PAYOUT_MAPPER, workspaceId, payoutRequestId);
    }

    public PayoutRequest markPayoutSucceeded(UUID workspaceId, UUID payoutRequestId) {
        return jdbcTemplate.queryForObject("""
                UPDATE payout_requests
                SET status = 'SUCCEEDED',
                    failure_reason = NULL
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('REQUESTED', 'PROCESSING')
                RETURNING *
                """, PAYOUT_MAPPER, workspaceId, payoutRequestId);
    }

    public PayoutRequest markPayoutFailed(UUID workspaceId, UUID payoutRequestId, String failureReason) {
        return jdbcTemplate.queryForObject("""
                UPDATE payout_requests
                SET status = 'FAILED',
                    failure_reason = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status IN ('REQUESTED', 'PROCESSING')
                RETURNING *
                """, PAYOUT_MAPPER, failureReason, workspaceId, payoutRequestId);
    }

    public void attachPayoutJournal(UUID workspaceId, UUID payoutRequestId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE payout_requests
                SET journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND journal_entry_id IS NULL
                """, journalEntryId, workspaceId, payoutRequestId);
    }

    public int nextPayoutAttemptNumber(UUID workspaceId, UUID payoutRequestId) {
        Integer result = jdbcTemplate.queryForObject("""
                SELECT COALESCE(MAX(attempt_number), 0) + 1
                FROM payout_attempts
                WHERE workspace_id = ?
                  AND payout_request_id = ?
                """, Integer.class, workspaceId, payoutRequestId);
        return result == null ? 1 : result;
    }

    public PayoutAttempt createPayoutAttempt(
            UUID workspaceId,
            UUID payoutRequestId,
            int attemptNumber,
            String status,
            String failureReason,
            String externalReference,
            UUID journalEntryId) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO payout_attempts (
                    workspace_id,
                    payout_request_id,
                    attempt_number,
                    status,
                    failure_reason,
                    external_reference,
                    journal_entry_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """, PAYOUT_ATTEMPT_MAPPER, workspaceId, payoutRequestId, attemptNumber, status, failureReason, externalReference, journalEntryId);
    }

    public void attachPayoutAttemptJournal(UUID workspaceId, UUID payoutAttemptId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE payout_attempts
                SET journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND journal_entry_id IS NULL
                """, journalEntryId, workspaceId, payoutAttemptId);
    }

    public List<PayoutAttempt> listPayoutAttempts(UUID workspaceId, UUID payoutRequestId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM payout_attempts
                WHERE workspace_id = ?
                  AND payout_request_id = ?
                ORDER BY attempt_number, created_at
                """, PAYOUT_ATTEMPT_MAPPER, workspaceId, payoutRequestId);
    }

    public RefundRecord createRefund(
            UUID workspaceId,
            UUID paymentIntentId,
            UUID bookingId,
            String refundType,
            long amountMinor,
            long payableReductionAmountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO refunds (
                    workspace_id,
                    payment_intent_id,
                    booking_id,
                    refund_type,
                    amount_minor,
                    payable_reduction_amount_minor
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING *
                """, REFUND_MAPPER, workspaceId, paymentIntentId, bookingId, refundType, amountMinor, payableReductionAmountMinor);
    }

    public void attachRefundJournal(UUID workspaceId, UUID refundId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE refunds
                SET journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND journal_entry_id IS NULL
                """, journalEntryId, workspaceId, refundId);
    }

    public RefundAttempt createRefundAttempt(UUID workspaceId, UUID refundId, String status, String failureReason, String externalReference) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO refund_attempts (
                    workspace_id,
                    refund_id,
                    status,
                    failure_reason,
                    external_reference
                )
                VALUES (?, ?, ?, ?, ?)
                RETURNING *
                """, REFUND_ATTEMPT_MAPPER, workspaceId, refundId, status, failureReason, externalReference);
    }

    public List<RefundRecord> listRefunds(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM refunds
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, REFUND_MAPPER, workspaceId);
    }

    public Optional<RefundRecord> findRefund(UUID workspaceId, UUID refundId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM refunds
                WHERE workspace_id = ?
                  AND id = ?
                """, REFUND_MAPPER, workspaceId, refundId).stream().findFirst();
    }

    public RefundRecord markRefundFailed(UUID workspaceId, UUID refundId) {
        return jdbcTemplate.queryForObject("""
                UPDATE refunds
                SET status = 'FAILED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'SUCCEEDED'
                RETURNING *
                """, REFUND_MAPPER, workspaceId, refundId);
    }

    public RefundRecord markRefundSucceeded(UUID workspaceId, UUID refundId) {
        return jdbcTemplate.queryForObject("""
                UPDATE refunds
                SET status = 'SUCCEEDED'
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'FAILED'
                RETURNING *
                """, REFUND_MAPPER, workspaceId, refundId);
    }

    public long refundedAmountForIntent(UUID workspaceId, UUID paymentIntentId) {
        Long value = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(amount_minor), 0)
                FROM refunds
                WHERE workspace_id = ?
                  AND payment_intent_id = ?
                  AND status = 'SUCCEEDED'
                """, Long.class, workspaceId, paymentIntentId);
        return value == null ? 0 : value;
    }

    public long payableReductionForIntent(UUID workspaceId, UUID paymentIntentId) {
        Long value = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(payable_reduction_amount_minor), 0)
                FROM refunds
                WHERE workspace_id = ?
                  AND payment_intent_id = ?
                  AND status = 'SUCCEEDED'
                """, Long.class, workspaceId, paymentIntentId);
        return value == null ? 0 : value;
    }

    public long activeDisputeExposureForIntent(UUID workspaceId, UUID paymentIntentId) {
        Long value = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(disputed_amount_minor), 0)
                FROM disputes
                WHERE workspace_id = ?
                  AND payment_intent_id = ?
                  AND status IN ('OPEN', 'LOST')
                """, Long.class, workspaceId, paymentIntentId);
        return value == null ? 0 : value;
    }

    public long activeDisputePayableExposureForIntent(UUID workspaceId, UUID paymentIntentId) {
        Long value = jdbcTemplate.queryForObject("""
                SELECT COALESCE(SUM(frozen_amount_minor), 0)
                FROM disputes
                WHERE workspace_id = ?
                  AND payment_intent_id = ?
                  AND status IN ('OPEN', 'LOST')
                """, Long.class, workspaceId, paymentIntentId);
        return value == null ? 0 : value;
    }

    public DisputeRecord createDispute(UUID workspaceId, UUID paymentIntentId, UUID bookingId, long disputedAmountMinor, long frozenAmountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO disputes (
                    workspace_id,
                    payment_intent_id,
                    booking_id,
                    disputed_amount_minor,
                    frozen_amount_minor
                )
                VALUES (?, ?, ?, ?, ?)
                RETURNING *
                """, DISPUTE_MAPPER, workspaceId, paymentIntentId, bookingId, disputedAmountMinor, frozenAmountMinor);
    }

    public Optional<DisputeRecord> findDispute(UUID workspaceId, UUID disputeId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM disputes
                WHERE workspace_id = ?
                  AND id = ?
                """, DISPUTE_MAPPER, workspaceId, disputeId).stream().findFirst();
    }

    public List<DisputeRecord> listDisputes(UUID workspaceId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM disputes
                WHERE workspace_id = ?
                ORDER BY created_at, id
                """, DISPUTE_MAPPER, workspaceId);
    }

    public FrozenFund createFrozenFund(UUID workspaceId, UUID providerProfileId, UUID disputeId, String currencyCode, long amountMinor) {
        return jdbcTemplate.queryForObject("""
                INSERT INTO frozen_funds (
                    workspace_id,
                    provider_profile_id,
                    dispute_id,
                    currency_code,
                    amount_minor
                )
                VALUES (?, ?, ?, ?, ?)
                RETURNING *
                """, FROZEN_MAPPER, workspaceId, providerProfileId, disputeId, currencyCode, amountMinor);
    }

    public Optional<FrozenFund> findFrozenFundForDispute(UUID workspaceId, UUID disputeId) {
        return jdbcTemplate.query("""
                SELECT *
                FROM frozen_funds
                WHERE workspace_id = ?
                  AND dispute_id = ?
                """, FROZEN_MAPPER, workspaceId, disputeId).stream().findFirst();
    }

    public FrozenFund updateFrozenFundStatus(UUID workspaceId, UUID frozenFundId, String status) {
        return jdbcTemplate.queryForObject("""
                UPDATE frozen_funds
                SET status = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'FROZEN'
                RETURNING *
                """, FROZEN_MAPPER, status, workspaceId, frozenFundId);
    }

    public DisputeRecord resolveDispute(UUID workspaceId, UUID disputeId, String status, String resolution) {
        return jdbcTemplate.queryForObject("""
                UPDATE disputes
                SET status = ?,
                    resolution = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND status = 'OPEN'
                RETURNING *
                """, DISPUTE_MAPPER, status, resolution, workspaceId, disputeId);
    }

    public void attachDisputeOpenJournal(UUID workspaceId, UUID disputeId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE disputes
                SET open_journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND open_journal_entry_id IS NULL
                """, journalEntryId, workspaceId, disputeId);
    }

    public void attachDisputeResolveJournal(UUID workspaceId, UUID disputeId, UUID journalEntryId) {
        jdbcTemplate.update("""
                UPDATE disputes
                SET resolve_journal_entry_id = ?
                WHERE workspace_id = ?
                  AND id = ?
                  AND resolve_journal_entry_id IS NULL
                """, journalEntryId, workspaceId, disputeId);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getTimestamp(column).toInstant();
    }
}
