package com.kayledger.api.payment.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.booking.model.Booking;
import com.kayledger.api.booking.store.BookingStore;
import com.kayledger.api.finance.application.FinanceService;
import com.kayledger.api.finance.application.FinanceService.CreateJournalEntryCommand;
import com.kayledger.api.finance.application.FinanceService.PostingCommand;
import com.kayledger.api.finance.model.FinancialAccount;
import com.kayledger.api.finance.model.JournalEntryDetails;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.PaymentIntentDetails;
import com.kayledger.api.payment.model.PaymentAttempt;
import com.kayledger.api.payment.model.DisputeRecord;
import com.kayledger.api.payment.model.FrozenFund;
import com.kayledger.api.payment.model.PayoutAttempt;
import com.kayledger.api.payment.model.PayoutRequest;
import com.kayledger.api.payment.model.ProviderBalanceSummary;
import com.kayledger.api.payment.model.RefundRecord;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;
import com.kayledger.api.subscription.model.SubscriptionCycle;
import com.kayledger.api.subscription.store.SubscriptionStore;

@Service
public class PaymentService {

    private static final String HELD = "HELD";
    private static final String CREATED = "CREATED";
    private static final String AUTHORIZED = "AUTHORIZED";
    private static final String CAPTURED = "CAPTURED";
    private static final String SETTLED = "SETTLED";
    private static final Set<String> DISPUTE_RESOLUTIONS = Set.of("WON", "LOST", "CLOSED");

    private final PaymentStore paymentStore;
    private final BookingStore bookingStore;
    private final AccessPolicy accessPolicy;
    private final FinanceService financeService;
    private final SubscriptionStore subscriptionStore;

    public PaymentService(PaymentStore paymentStore, BookingStore bookingStore, AccessPolicy accessPolicy, FinanceService financeService, SubscriptionStore subscriptionStore) {
        this.paymentStore = paymentStore;
        this.bookingStore = bookingStore;
        this.accessPolicy = accessPolicy;
        this.financeService = financeService;
        this.subscriptionStore = subscriptionStore;
    }

    @Transactional
    public PaymentIntentDetails createIntent(AccessContext context, CreatePaymentIntentCommand command) {
        requirePaymentWrite(context);
        Booking booking = booking(context, requireId(command.bookingId(), "bookingId"));
        requireBookingFinancialSnapshot(booking);
        if (!HELD.equals(booking.status())) {
            throw new BadRequestException("Payment intent can only be created for a held booking.");
        }
        PaymentIntent intent = paymentStore.createIntent(
                context.workspaceId(),
                booking.id(),
                booking.providerProfileId(),
                booking.currencyCode(),
                booking.grossAmountMinor(),
                booking.feeAmountMinor(),
                booking.netAmountMinor(),
                blankToNull(command.externalReference()));
        requireIntentMatchesBooking(intent, booking);
        return details(context.workspaceId(), intent);
    }

    public List<PaymentIntent> list(AccessContext context) {
        requirePaymentRead(context);
        return paymentStore.list(context.workspaceId());
    }

    public PaymentIntentDetails findByBooking(AccessContext context, UUID bookingId) {
        requirePaymentRead(context);
        PaymentIntent intent = paymentStore.findByBooking(context.workspaceId(), requireId(bookingId, "bookingId"))
                .orElseThrow(() -> new NotFoundException("Payment intent was not found."));
        return details(context.workspaceId(), intent);
    }

    public List<PaymentIntent> listBySubscription(AccessContext context, UUID subscriptionId) {
        requirePaymentRead(context);
        return paymentStore.listBySubscription(context.workspaceId(), requireId(subscriptionId, "subscriptionId"));
    }

    public List<PaymentIntent> listBySubscriptionCycle(AccessContext context, UUID subscriptionCycleId) {
        requirePaymentRead(context);
        return paymentStore.listBySubscriptionCycle(context.workspaceId(), requireId(subscriptionCycleId, "subscriptionCycleId"));
    }

    @Transactional
    public PaymentIntentDetails authorize(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        if (!CREATED.equals(existing.status())) {
            throw new BadRequestException("Only created payment intents can be authorized.");
        }
        long amount = amountOrDefault(command, existing.grossAmountMinor());
        if (amount > existing.grossAmountMinor()) {
            throw new BadRequestException("Authorization cannot exceed the payment gross amount.");
        }
        try {
            PaymentIntent authorized = paymentStore.authorize(context.workspaceId(), existing.id(), amount);
            PaymentAttempt attempt = paymentStore.createAttempt(context.workspaceId(), authorized.id(), "AUTHORIZE", "SUCCEEDED", amount, externalReference(command), null);
            attachJournal(context.workspaceId(), authorized, attempt, "Payment authorization held funds posture", List.of(
                    posting(account(context.workspaceId(), "AUTHORIZED_FUNDS", authorized.currencyCode()), "DEBIT", amount, authorized.currencyCode()),
                    posting(account(context.workspaceId(), "PLATFORM_CLEARING", authorized.currencyCode()), "CREDIT", amount, authorized.currencyCode())));
            return details(context.workspaceId(), authorized);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be authorized.");
        }
    }

    @Transactional
    public PaymentIntentDetails capture(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        if (!AUTHORIZED.equals(existing.status())) {
            throw new BadRequestException("Only authorized payment intents can be captured.");
        }
        long amount = amountOrDefault(command, existing.authorizedAmountMinor());
        if (amount > existing.authorizedAmountMinor()) {
            throw new BadRequestException("Capture cannot exceed authorized amount.");
        }
        if (amount != existing.authorizedAmountMinor()) {
            throw new BadRequestException("Partial capture is not supported in this foundation slice.");
        }
        try {
            PaymentIntent captured = paymentStore.capture(context.workspaceId(), existing.id(), amount);
            PaymentAttempt attempt = paymentStore.createAttempt(context.workspaceId(), captured.id(), "CAPTURE", "SUCCEEDED", amount, externalReference(command), null);
            attachJournal(context.workspaceId(), captured, attempt, "Payment capture into clearing posture", List.of(
                    posting(account(context.workspaceId(), "PLATFORM_CLEARING", captured.currencyCode()), "DEBIT", amount, captured.currencyCode()),
                    posting(account(context.workspaceId(), "CAPTURED_FUNDS", captured.currencyCode()), "DEBIT", amount, captured.currencyCode()),
                    posting(account(context.workspaceId(), "AUTHORIZED_FUNDS", captured.currencyCode()), "CREDIT", amount, captured.currencyCode()),
                    posting(account(context.workspaceId(), "PLATFORM_CLEARING", captured.currencyCode()), "CREDIT", amount, captured.currencyCode())));
            return details(context.workspaceId(), captured);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be captured.");
        }
    }

    @Transactional
    public PaymentIntentDetails settle(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        if (!CAPTURED.equals(existing.status())) {
            throw new BadRequestException("Only captured payment intents can be settled.");
        }
        long amount = amountOrDefault(command, existing.capturedAmountMinor());
        if (amount > existing.capturedAmountMinor()) {
            throw new BadRequestException("Settlement cannot exceed captured amount.");
        }
        if (amount != existing.capturedAmountMinor() || amount != existing.grossAmountMinor()) {
            throw new BadRequestException("Partial settlement is not supported in this foundation slice.");
        }
        try {
            PaymentIntent settled = paymentStore.settle(context.workspaceId(), existing.id(), amount);
            PaymentAttempt attempt = paymentStore.createAttempt(context.workspaceId(), settled.id(), "SETTLE", "SUCCEEDED", amount, externalReference(command), null);
            attachJournal(context.workspaceId(), settled, attempt, "Payment settlement creates payable and fee revenue", settlementPostings(context.workspaceId(), settled, amount));
            paymentStore.refreshPayableBalance(
                    context.workspaceId(),
                    settled.providerProfileId(),
                    settled.currencyCode());
            activateSubscriptionCycleIfPresent(context.workspaceId(), settled);
            return details(context.workspaceId(), settled);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be settled.");
        }
    }

    @Transactional
    public PaymentIntentDetails cancel(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        try {
            PaymentIntent cancelled = paymentStore.cancel(context.workspaceId(), existing.id());
            PaymentAttempt attempt = paymentStore.createAttempt(context.workspaceId(), cancelled.id(), "CANCEL", "SUCCEEDED", existing.authorizedAmountMinor(), externalReference(command), null);
            if (AUTHORIZED.equals(existing.status()) && existing.authorizedAmountMinor() > 0) {
                attachJournal(context.workspaceId(), cancelled, attempt, "Payment cancellation reverses authorization posture", List.of(
                        posting(account(context.workspaceId(), "PLATFORM_CLEARING", cancelled.currencyCode()), "DEBIT", existing.authorizedAmountMinor(), cancelled.currencyCode()),
                        posting(account(context.workspaceId(), "AUTHORIZED_FUNDS", cancelled.currencyCode()), "CREDIT", existing.authorizedAmountMinor(), cancelled.currencyCode())));
            }
            return details(context.workspaceId(), cancelled);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be cancelled.");
        }
    }

    @Transactional
    public PaymentIntentDetails requireAction(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        if (!CREATED.equals(existing.status())) {
            throw new BadRequestException("Only created payment intents can be moved to requires action.");
        }
        try {
            PaymentIntent requiresAction = paymentStore.requireAction(context.workspaceId(), existing.id());
            return details(context.workspaceId(), requiresAction);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be moved to requires action.");
        }
    }

    @Transactional
    public PaymentIntentDetails fail(AccessContext context, UUID paymentIntentId, AmountCommand command) {
        requirePaymentWrite(context);
        PaymentIntent existing = intent(context, paymentIntentId);
        if (AUTHORIZED.equals(existing.status()) || CAPTURED.equals(existing.status())) {
            throw new BadRequestException("Authorized or captured payment intents must be cancelled or settled through financial transitions.");
        }
        try {
            PaymentIntent failed = paymentStore.fail(context.workspaceId(), existing.id());
            paymentStore.createAttempt(context.workspaceId(), failed.id(), "AUTHORIZE", "FAILED", 0, externalReference(command), null);
            return details(context.workspaceId(), failed);
        } catch (EmptyResultDataAccessException exception) {
            throw new BadRequestException("Payment intent could not be failed.");
        }
    }

    public List<ProviderBalanceSummary> listPayableBalances(AccessContext context, UUID providerProfileId) {
        requirePaymentRead(context);
        if (providerProfileId == null) {
            return paymentStore.listProviderBalanceSummaries(context.workspaceId());
        }
        return paymentStore.listProviderBalanceSummaries(context.workspaceId()).stream()
                .filter(summary -> summary.providerProfileId().equals(providerProfileId))
                .toList();
    }

    public List<ProviderBalanceSummary> listProviderBalanceSummaries(AccessContext context) {
        requirePaymentRead(context);
        return paymentStore.listProviderBalanceSummaries(context.workspaceId());
    }

    @Transactional
    public PayoutRequest requestPayout(AccessContext context, PayoutRequestCommand command) {
        requirePaymentWrite(context);
        if (command == null) {
            throw new BadRequestException("Payout request body is required.");
        }
        UUID providerProfileId = requireId(command.providerProfileId(), "providerProfileId");
        String currencyCode = requireCurrency(command.currencyCode());
        long amountMinor = requirePositive(command.amountMinor(), "amountMinor");
        ProviderBalanceSummary summary = paymentStore.balanceSummary(context.workspaceId(), providerProfileId, currencyCode);
        if (amountMinor > summary.availableAmountMinor()) {
            throw new BadRequestException("Payout cannot exceed available payable balance.");
        }
        PayoutRequest payout = paymentStore.createPayoutRequest(context.workspaceId(), providerProfileId, currencyCode, amountMinor);
        return attachPayoutRequestJournal(context.workspaceId(), payout, "Payout request reserves seller payable for payout clearing", List.of(
                posting(account(context.workspaceId(), "SELLER_PAYABLE", payout.currencyCode()), "DEBIT", payout.requestedAmountMinor(), payout.currencyCode()),
                posting(account(context.workspaceId(), "PAYOUT_CLEARING", payout.currencyCode()), "CREDIT", payout.requestedAmountMinor(), payout.currencyCode())));
    }

    public List<PayoutRequest> listPayouts(AccessContext context) {
        requirePaymentRead(context);
        return paymentStore.listPayouts(context.workspaceId());
    }

    public List<PayoutAttempt> listPayoutAttempts(AccessContext context, UUID payoutRequestId) {
        requirePaymentRead(context);
        PayoutRequest payout = payout(context, payoutRequestId);
        return paymentStore.listPayoutAttempts(context.workspaceId(), payout.id());
    }

    @Transactional
    public PayoutRequest markPayoutSucceeded(AccessContext context, UUID payoutRequestId, PayoutMutationCommand command) {
        requirePaymentWrite(context);
        PayoutRequest payout = payout(context, payoutRequestId);
        if (!"REQUESTED".equals(payout.status()) && !"PROCESSING".equals(payout.status())) {
            throw new BadRequestException("Only requested or processing payouts can succeed.");
        }
        int attemptNumber = paymentStore.nextPayoutAttemptNumber(context.workspaceId(), payout.id());
        PayoutRequest succeeded = paymentStore.markPayoutSucceeded(context.workspaceId(), payout.id());
        PayoutAttempt attempt = paymentStore.createPayoutAttempt(context.workspaceId(), payout.id(), attemptNumber, "SUCCEEDED", null, externalReference(command), null);
        attachPayoutAttemptJournal(context.workspaceId(), succeeded, attempt, "Payout success clears payout clearing through cash placeholder", List.of(
                posting(account(context.workspaceId(), "PAYOUT_CLEARING", succeeded.currencyCode()), "DEBIT", succeeded.requestedAmountMinor(), succeeded.currencyCode()),
                posting(account(context.workspaceId(), "CASH_PLACEHOLDER", succeeded.currencyCode()), "CREDIT", succeeded.requestedAmountMinor(), succeeded.currencyCode())));
        return succeeded;
    }

    @Transactional
    public PayoutRequest markPayoutFailed(AccessContext context, UUID payoutRequestId, PayoutMutationCommand command) {
        requirePaymentWrite(context);
        PayoutRequest payout = payout(context, payoutRequestId);
        if (!"REQUESTED".equals(payout.status()) && !"PROCESSING".equals(payout.status())) {
            throw new BadRequestException("Only requested or processing payouts can fail.");
        }
        int attemptNumber = paymentStore.nextPayoutAttemptNumber(context.workspaceId(), payout.id());
        PayoutRequest failed = paymentStore.markPayoutFailed(context.workspaceId(), payout.id(), requireText(command == null ? null : command.failureReason(), "failureReason"));
        PayoutAttempt attempt = paymentStore.createPayoutAttempt(context.workspaceId(), payout.id(), attemptNumber, "FAILED", failed.failureReason(), externalReference(command), null);
        attachPayoutAttemptJournal(context.workspaceId(), failed, attempt, "Payout failure restores seller payable from payout clearing", List.of(
                posting(account(context.workspaceId(), "PAYOUT_CLEARING", failed.currencyCode()), "DEBIT", failed.requestedAmountMinor(), failed.currencyCode()),
                posting(account(context.workspaceId(), "SELLER_PAYABLE", failed.currencyCode()), "CREDIT", failed.requestedAmountMinor(), failed.currencyCode())));
        return failed;
    }

    @Transactional
    public PayoutRequest retryPayout(AccessContext context, UUID payoutRequestId, PayoutMutationCommand command) {
        requirePaymentWrite(context);
        PayoutRequest payout = payout(context, payoutRequestId);
        if (!"FAILED".equals(payout.status())) {
            throw new BadRequestException("Only failed payouts can be retried.");
        }
        ProviderBalanceSummary summary = paymentStore.balanceSummary(context.workspaceId(), payout.providerProfileId(), payout.currencyCode());
        if (payout.requestedAmountMinor() > summary.availableAmountMinor()) {
            throw new BadRequestException("Retried payout cannot exceed available payable balance.");
        }
        int attemptNumber = paymentStore.nextPayoutAttemptNumber(context.workspaceId(), payout.id());
        PayoutRequest processing = paymentStore.markPayoutProcessing(context.workspaceId(), payout.id());
        PayoutAttempt attempt = paymentStore.createPayoutAttempt(context.workspaceId(), payout.id(), attemptNumber, "PROCESSING", null, externalReference(command), null);
        attachPayoutAttemptJournal(context.workspaceId(), processing, attempt, "Payout retry reserves seller payable for payout clearing", List.of(
                posting(account(context.workspaceId(), "SELLER_PAYABLE", processing.currencyCode()), "DEBIT", processing.requestedAmountMinor(), processing.currencyCode()),
                posting(account(context.workspaceId(), "PAYOUT_CLEARING", processing.currencyCode()), "CREDIT", processing.requestedAmountMinor(), processing.currencyCode())));
        return processing;
    }

    @Transactional
    public RefundRecord createFullRefund(AccessContext context, RefundCommand command) {
        return createRefund(context, "FULL", command);
    }

    @Transactional
    public RefundRecord createPartialRefund(AccessContext context, RefundCommand command) {
        return createRefund(context, "PARTIAL", command);
    }

    @Transactional
    public RefundRecord createReversal(AccessContext context, RefundCommand command) {
        return createRefund(context, "REVERSAL", command);
    }

    public List<RefundRecord> listRefunds(AccessContext context) {
        requirePaymentRead(context);
        return paymentStore.listRefunds(context.workspaceId());
    }

    @Transactional
    public RefundRecord markRefundFailed(AccessContext context, UUID refundId, RefundFailureCommand command) {
        requirePaymentWrite(context);
        RefundRecord refund = refund(context, refundId);
        PaymentIntent intent = intent(context, refund.paymentIntentId());
        RefundRecord failed = paymentStore.markRefundFailed(context.workspaceId(), refund.id());
        paymentStore.createRefundAttempt(context.workspaceId(), failed.id(), "FAILED", requireText(command == null ? null : command.failureReason(), "failureReason"), externalReference(command));
        attachRefundJournal(context.workspaceId(), failed, intent, "Refund failure reverses prior refund posture", reverseRefundPostings(context.workspaceId(), intent, refund.refundType(), refund.amountMinor(), refund.payableReductionAmountMinor()));
        return failed;
    }

    @Transactional
    public RefundRecord retryRefund(AccessContext context, UUID refundId, RefundFailureCommand command) {
        requirePaymentWrite(context);
        RefundRecord refund = refund(context, refundId);
        PaymentIntent intent = intent(context, refund.paymentIntentId());
        long alreadyRefunded = paymentStore.refundedAmountForIntent(context.workspaceId(), intent.id());
        long alreadyDisputed = paymentStore.activeDisputeExposureForIntent(context.workspaceId(), intent.id());
        if (refund.amountMinor() > intent.grossAmountMinor() - alreadyRefunded - alreadyDisputed) {
            throw new BadRequestException("Retried refund amount exceeds remaining payment exposure.");
        }
        RefundRecord succeeded = paymentStore.markRefundSucceeded(context.workspaceId(), refund.id());
        paymentStore.createRefundAttempt(context.workspaceId(), succeeded.id(), "SUCCEEDED", null, externalReference(command));
        attachRefundJournal(context.workspaceId(), succeeded, intent, "Refund retry reapplies compensating payable and fee effects", refundPostings(context.workspaceId(), intent, succeeded.refundType(), succeeded.amountMinor(), succeeded.payableReductionAmountMinor()));
        return succeeded;
    }

    @Transactional
    public DisputeRecord openDispute(AccessContext context, DisputeCommand command) {
        requirePaymentWrite(context);
        if (command == null) {
            throw new BadRequestException("Dispute request body is required.");
        }
        PaymentIntent intent = intent(context, requireId(command.paymentIntentId(), "paymentIntentId"));
        if (!SETTLED.equals(intent.status())) {
            throw new BadRequestException("Disputes require a settled payment intent.");
        }
        long amountMinor = requirePositive(command.amountMinor(), "amountMinor");
        long alreadyRefunded = paymentStore.refundedAmountForIntent(context.workspaceId(), intent.id());
        long alreadyDisputed = paymentStore.activeDisputeExposureForIntent(context.workspaceId(), intent.id());
        long remainingDisputable = intent.grossAmountMinor() - alreadyRefunded - alreadyDisputed;
        if (amountMinor > remainingDisputable) {
            throw new BadRequestException("Dispute amount exceeds remaining payment exposure.");
        }
        ProviderBalanceSummary summary = paymentStore.balanceSummary(context.workspaceId(), intent.providerProfileId(), intent.currencyCode());
        if (amountMinor > summary.availableAmountMinor()) {
            throw new BadRequestException("Dispute cannot freeze more than available payable balance.");
        }
        DisputeRecord dispute = paymentStore.createDispute(context.workspaceId(), intent.id(), intent.bookingId(), amountMinor, amountMinor);
        paymentStore.createFrozenFund(context.workspaceId(), intent.providerProfileId(), dispute.id(), intent.currencyCode(), amountMinor);
        return attachDisputeOpenJournal(context.workspaceId(), dispute, intent, "Dispute opening freezes seller payable", List.of(
                posting(account(context.workspaceId(), "SELLER_PAYABLE", intent.currencyCode()), "DEBIT", amountMinor, intent.currencyCode()),
                posting(account(context.workspaceId(), "FROZEN_PAYABLE", intent.currencyCode()), "CREDIT", amountMinor, intent.currencyCode())));
    }

    @Transactional
    public DisputeRecord resolveDispute(AccessContext context, UUID disputeId, ResolveDisputeCommand command) {
        requirePaymentWrite(context);
        DisputeRecord dispute = dispute(context, disputeId);
        String resolution = requireOneOf(command == null ? null : command.resolution(), DISPUTE_RESOLUTIONS, "resolution");
        FrozenFund frozenFund = paymentStore.findFrozenFundForDispute(context.workspaceId(), dispute.id())
                .orElseThrow(() -> new BadRequestException("Dispute has no frozen funds."));
        String frozenStatus = "LOST".equals(resolution) ? "CONSUMED" : "RELEASED";
        paymentStore.updateFrozenFundStatus(context.workspaceId(), frozenFund.id(), frozenStatus);
        DisputeRecord resolved = paymentStore.resolveDispute(context.workspaceId(), dispute.id(), resolution, resolution);
        String creditPurpose = "LOST".equals(resolution) ? "DISPUTE_RESERVE" : "SELLER_PAYABLE";
        return attachDisputeResolveJournal(context.workspaceId(), resolved, frozenFund, "Dispute resolution updates frozen payable posture", List.of(
                posting(account(context.workspaceId(), "FROZEN_PAYABLE", frozenFund.currencyCode()), "DEBIT", frozenFund.amountMinor(), frozenFund.currencyCode()),
                posting(account(context.workspaceId(), creditPurpose, frozenFund.currencyCode()), "CREDIT", frozenFund.amountMinor(), frozenFund.currencyCode())));
    }

    public List<DisputeRecord> listDisputes(AccessContext context) {
        requirePaymentRead(context);
        return paymentStore.listDisputes(context.workspaceId());
    }

    private RefundRecord createRefund(AccessContext context, String refundType, RefundCommand command) {
        requirePaymentWrite(context);
        if (command == null) {
            throw new BadRequestException("Refund request body is required.");
        }
        PaymentIntent intent = intent(context, requireId(command.paymentIntentId(), "paymentIntentId"));
        if (!SETTLED.equals(intent.status())) {
            throw new BadRequestException("Refunds and reversals require a settled payment intent.");
        }
        long alreadyRefunded = paymentStore.refundedAmountForIntent(context.workspaceId(), intent.id());
        long alreadyDisputed = paymentStore.activeDisputeExposureForIntent(context.workspaceId(), intent.id());
        long remainingRefundable = intent.grossAmountMinor() - alreadyRefunded - alreadyDisputed;
        long amountMinor;
        if ("FULL".equals(refundType)) {
            amountMinor = remainingRefundable;
        } else {
            amountMinor = requirePositive(command.amountMinor(), "amountMinor");
        }
        if (amountMinor <= 0 || amountMinor > remainingRefundable) {
            throw new BadRequestException("Refund amount exceeds remaining refundable amount.");
        }
        long priorPayableReduction = paymentStore.payableReductionForIntent(context.workspaceId(), intent.id())
                + paymentStore.activeDisputePayableExposureForIntent(context.workspaceId(), intent.id());
        long remainingPayableExposure = Math.max(intent.netAmountMinor() - priorPayableReduction, 0);
        long payableReduction = Math.min(amountMinor, remainingPayableExposure);
        RefundRecord refund = paymentStore.createRefund(context.workspaceId(), intent.id(), intent.bookingId(), refundType, amountMinor, payableReduction);
        paymentStore.createRefundAttempt(context.workspaceId(), refund.id(), "SUCCEEDED", null, externalReference(command));
        return attachRefundJournal(context.workspaceId(), refund, intent, refundDescription(refundType), refundPostings(context.workspaceId(), intent, refundType, amountMinor, payableReduction));
    }

    private PaymentIntentDetails details(UUID workspaceId, PaymentIntent intent) {
        return new PaymentIntentDetails(intent, paymentStore.listAttempts(workspaceId, intent.id()));
    }

    private List<PostingCommand> settlementPostings(UUID workspaceId, PaymentIntent settled, long amount) {
        List<PostingCommand> postings = new ArrayList<>();
        postings.add(posting(account(workspaceId, "PLATFORM_CLEARING", settled.currencyCode()), "DEBIT", amount, settled.currencyCode()));
        postings.add(posting(account(workspaceId, "CASH_PLACEHOLDER", settled.currencyCode()), "DEBIT", amount, settled.currencyCode()));
        postings.add(posting(account(workspaceId, "CAPTURED_FUNDS", settled.currencyCode()), "CREDIT", amount, settled.currencyCode()));
        if (settled.netAmountMinor() > 0) {
            postings.add(posting(account(workspaceId, "SELLER_PAYABLE", settled.currencyCode()), "CREDIT", settled.netAmountMinor(), settled.currencyCode()));
        }
        if (settled.feeAmountMinor() > 0) {
            postings.add(posting(account(workspaceId, "FEE_REVENUE", settled.currencyCode()), "CREDIT", settled.feeAmountMinor(), settled.currencyCode()));
        }
        return postings;
    }

    private void activateSubscriptionCycleIfPresent(UUID workspaceId, PaymentIntent settled) {
        if (settled.subscriptionCycleId() == null) {
            return;
        }
        SubscriptionCycle cycle = subscriptionStore.findCycleByPaymentIntent(workspaceId, settled.id())
                .orElseThrow(() -> new BadRequestException("Settled subscription payment intent is not linked to a subscription cycle."));
        SubscriptionCycle paid = subscriptionStore.markCyclePaid(workspaceId, cycle.id());
        subscriptionStore.advancePeriod(
                workspaceId,
                paid.subscriptionId(),
                paid.planId(),
                paid.providerProfileId(),
                paid.cycleStartAt(),
                paid.cycleEndAt());
        subscriptionStore.pendingPlanChange(workspaceId, paid.subscriptionId(), paid.cycleNumber(), paid.cycleStartAt())
                .ifPresent(change -> subscriptionStore.markPlanChangeApplied(workspaceId, change.id()));
        subscriptionStore.upsertEntitlement(
                workspaceId,
                paid.subscriptionId(),
                paid.customerProfileId(),
                "ACTIVE",
                paid.cycleStartAt(),
                paid.cycleEndAt());
    }

    private void attachJournal(UUID workspaceId, PaymentIntent intent, PaymentAttempt attempt, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "PAYMENT",
                        intent.id(),
                        null,
                        intent.bookingId(),
                        attempt.id().toString(),
                        description,
                        postings));
        paymentStore.attachAttemptJournal(workspaceId, attempt.id(), journal.journalEntry().id());
    }

    private PayoutRequest attachPayoutRequestJournal(UUID workspaceId, PayoutRequest payout, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "PAYOUT",
                        payout.id(),
                        null,
                        null,
                        null,
                        description,
                        postings));
        paymentStore.attachPayoutJournal(workspaceId, payout.id(), journal.journalEntry().id());
        return paymentStore.findPayout(workspaceId, payout.id()).orElse(payout);
    }

    private void attachPayoutAttemptJournal(UUID workspaceId, PayoutRequest payout, PayoutAttempt attempt, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "PAYOUT",
                        payout.id(),
                        null,
                        null,
                        attempt.id().toString(),
                        description,
                        postings));
        paymentStore.attachPayoutAttemptJournal(workspaceId, attempt.id(), journal.journalEntry().id());
    }

    private RefundRecord attachRefundJournal(UUID workspaceId, RefundRecord refund, PaymentIntent intent, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "REFUND",
                        refund.id(),
                        null,
                        intent.bookingId(),
                        intent.id().toString(),
                        description,
                        postings));
        paymentStore.attachRefundJournal(workspaceId, refund.id(), journal.journalEntry().id());
        return refund;
    }

    private DisputeRecord attachDisputeOpenJournal(UUID workspaceId, DisputeRecord dispute, PaymentIntent intent, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "DISPUTE",
                        dispute.id(),
                        null,
                        intent.bookingId(),
                        intent.id().toString(),
                        description,
                        postings));
        paymentStore.attachDisputeOpenJournal(workspaceId, dispute.id(), journal.journalEntry().id());
        return paymentStore.findDispute(workspaceId, dispute.id()).orElse(dispute);
    }

    private DisputeRecord attachDisputeResolveJournal(UUID workspaceId, DisputeRecord dispute, FrozenFund frozenFund, String description, List<PostingCommand> postings) {
        JournalEntryDetails journal = financeService.createJournalEntryForReference(
                workspaceId,
                new CreateJournalEntryCommand(
                        "DISPUTE",
                        dispute.id(),
                        null,
                        dispute.bookingId(),
                        frozenFund.id().toString(),
                        description,
                        postings));
        paymentStore.attachDisputeResolveJournal(workspaceId, dispute.id(), journal.journalEntry().id());
        return paymentStore.findDispute(workspaceId, dispute.id()).orElse(dispute);
    }

    private String refundDescription(String refundType) {
        if ("REVERSAL".equals(refundType)) {
            return "Reversal creates compensating payable and liability effects";
        }
        return "Refund creates compensating payable and fee effects";
    }

    private List<PostingCommand> refundPostings(UUID workspaceId, PaymentIntent intent, String refundType, long amountMinor, long payableReduction) {
        long feeReduction = amountMinor - payableReduction;
        List<PostingCommand> postings = new ArrayList<>();
        if (payableReduction > 0) {
            postings.add(posting(account(workspaceId, "SELLER_PAYABLE", intent.currencyCode()), "DEBIT", payableReduction, intent.currencyCode()));
        }
        if (feeReduction > 0) {
            postings.add(posting(account(workspaceId, "FEE_REVENUE", intent.currencyCode()), "DEBIT", feeReduction, intent.currencyCode()));
        }
        String liabilityPurpose = "REVERSAL".equals(refundType) ? "REFUND_LIABILITY" : "REFUND_RESERVE";
        postings.add(posting(account(workspaceId, liabilityPurpose, intent.currencyCode()), "CREDIT", amountMinor, intent.currencyCode()));
        return postings;
    }

    private List<PostingCommand> reverseRefundPostings(UUID workspaceId, PaymentIntent intent, String refundType, long amountMinor, long payableReduction) {
        long feeReduction = amountMinor - payableReduction;
        String liabilityPurpose = "REVERSAL".equals(refundType) ? "REFUND_LIABILITY" : "REFUND_RESERVE";
        List<PostingCommand> postings = new ArrayList<>();
        postings.add(posting(account(workspaceId, liabilityPurpose, intent.currencyCode()), "DEBIT", amountMinor, intent.currencyCode()));
        if (payableReduction > 0) {
            postings.add(posting(account(workspaceId, "SELLER_PAYABLE", intent.currencyCode()), "CREDIT", payableReduction, intent.currencyCode()));
        }
        if (feeReduction > 0) {
            postings.add(posting(account(workspaceId, "FEE_REVENUE", intent.currencyCode()), "CREDIT", feeReduction, intent.currencyCode()));
        }
        return postings;
    }

    private FinancialAccount account(UUID workspaceId, String purpose, String currencyCode) {
        return financeService.accountByPurpose(workspaceId, purpose, currencyCode);
    }

    private PostingCommand posting(FinancialAccount account, String side, long amountMinor, String currencyCode) {
        return new PostingCommand(account.id(), side, amountMinor, currencyCode);
    }

    private PaymentIntent intent(AccessContext context, UUID paymentIntentId) {
        return paymentStore.find(context.workspaceId(), requireId(paymentIntentId, "paymentIntentId"))
                .orElseThrow(() -> new NotFoundException("Payment intent was not found."));
    }

    private PayoutRequest payout(AccessContext context, UUID payoutRequestId) {
        return paymentStore.findPayout(context.workspaceId(), requireId(payoutRequestId, "payoutRequestId"))
                .orElseThrow(() -> new NotFoundException("Payout request was not found."));
    }

    private DisputeRecord dispute(AccessContext context, UUID disputeId) {
        return paymentStore.findDispute(context.workspaceId(), requireId(disputeId, "disputeId"))
                .orElseThrow(() -> new NotFoundException("Dispute was not found."));
    }

    private RefundRecord refund(AccessContext context, UUID refundId) {
        return paymentStore.findRefund(context.workspaceId(), requireId(refundId, "refundId"))
                .orElseThrow(() -> new NotFoundException("Refund was not found."));
    }

    private Booking booking(AccessContext context, UUID bookingId) {
        return bookingStore.find(context.workspaceId(), bookingId)
                .orElseThrow(() -> new NotFoundException("Booking was not found."));
    }

    private void requireIntentMatchesBooking(PaymentIntent intent, Booking booking) {
        if (!intent.bookingId().equals(booking.id())
                || !intent.providerProfileId().equals(booking.providerProfileId())
                || !intent.currencyCode().equals(booking.currencyCode())
                || intent.grossAmountMinor() != booking.grossAmountMinor()
                || intent.feeAmountMinor() != booking.feeAmountMinor()
                || intent.netAmountMinor() != booking.netAmountMinor()) {
            throw new BadRequestException("Payment intent amounts must match the booking financial snapshot.");
        }
    }

    private void requireBookingFinancialSnapshot(Booking booking) {
        if (booking.currencyCode() == null
                || booking.grossAmountMinor() == null
                || booking.feeAmountMinor() == null
                || booking.netAmountMinor() == null) {
            throw new BadRequestException("Booking has no financial snapshot.");
        }
    }

    private void requirePaymentRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requirePaymentWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private static UUID requireId(UUID value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static long amountOrDefault(AmountCommand command, long defaultAmount) {
        if (command == null || command.amountMinor() == null) {
            return defaultAmount;
        }
        if (command.amountMinor() <= 0) {
            throw new BadRequestException("amountMinor must be greater than zero.");
        }
        return command.amountMinor();
    }

    private static String externalReference(AmountCommand command) {
        return command == null ? null : blankToNull(command.externalReference());
    }

    private static String externalReference(PayoutMutationCommand command) {
        return command == null ? null : blankToNull(command.externalReference());
    }

    private static String externalReference(RefundCommand command) {
        return command == null ? null : blankToNull(command.externalReference());
    }

    private static String externalReference(RefundFailureCommand command) {
        return command == null ? null : blankToNull(command.externalReference());
    }

    private static long requirePositive(Long value, String field) {
        if (value == null || value <= 0) {
            throw new BadRequestException(field + " must be greater than zero.");
        }
        return value;
    }

    private static String requireCurrency(String value) {
        String currency = requireText(value, "currencyCode").toUpperCase();
        if (!currency.matches("[A-Z]{3}")) {
            throw new BadRequestException("currencyCode must be a three-letter ISO code.");
        }
        return currency;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireOneOf(String value, Set<String> allowed, String field) {
        String required = requireText(value, field);
        if (!allowed.contains(required)) {
            throw new BadRequestException(field + " is invalid.");
        }
        return required;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public record CreatePaymentIntentCommand(UUID bookingId, String externalReference) {
    }

    public record AmountCommand(Long amountMinor, String externalReference) {
    }

    public record PayoutRequestCommand(UUID providerProfileId, String currencyCode, Long amountMinor) {
    }

    public record PayoutMutationCommand(String failureReason, String externalReference) {
    }

    public record RefundCommand(UUID paymentIntentId, Long amountMinor, String externalReference) {
    }

    public record RefundFailureCommand(String failureReason, String externalReference) {
    }

    public record DisputeCommand(UUID paymentIntentId, Long amountMinor) {
    }

    public record ResolveDisputeCommand(String resolution, String note) {
    }
}
