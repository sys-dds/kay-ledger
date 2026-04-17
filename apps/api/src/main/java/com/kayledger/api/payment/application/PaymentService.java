package com.kayledger.api.payment.application;

import java.util.List;
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
import com.kayledger.api.payment.model.ProviderPayableBalance;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class PaymentService {

    private static final String HELD = "HELD";
    private static final String CREATED = "CREATED";
    private static final String AUTHORIZED = "AUTHORIZED";
    private static final String CAPTURED = "CAPTURED";

    private final PaymentStore paymentStore;
    private final BookingStore bookingStore;
    private final AccessPolicy accessPolicy;
    private final FinanceService financeService;

    public PaymentService(PaymentStore paymentStore, BookingStore bookingStore, AccessPolicy accessPolicy, FinanceService financeService) {
        this.paymentStore = paymentStore;
        this.bookingStore = bookingStore;
        this.accessPolicy = accessPolicy;
        this.financeService = financeService;
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
            attachJournal(context.workspaceId(), settled, attempt, "Payment settlement creates payable and fee revenue", List.of(
                    posting(account(context.workspaceId(), "PLATFORM_CLEARING", settled.currencyCode()), "DEBIT", amount, settled.currencyCode()),
                    posting(account(context.workspaceId(), "SELLER_PAYABLE", settled.currencyCode()), "CREDIT", settled.netAmountMinor(), settled.currencyCode()),
                    posting(account(context.workspaceId(), "FEE_REVENUE", settled.currencyCode()), "CREDIT", settled.feeAmountMinor(), settled.currencyCode())));
            paymentStore.refreshPayableBalance(
                    context.workspaceId(),
                    settled.providerProfileId(),
                    settled.currencyCode());
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

    public List<ProviderPayableBalance> listPayableBalances(AccessContext context, UUID providerProfileId) {
        requirePaymentRead(context);
        if (providerProfileId == null) {
            return paymentStore.listPayableBalances(context.workspaceId());
        }
        return paymentStore.listPayableBalancesForProvider(context.workspaceId(), providerProfileId);
    }

    private PaymentIntentDetails details(UUID workspaceId, PaymentIntent intent) {
        return new PaymentIntentDetails(intent, paymentStore.listAttempts(workspaceId, intent.id()));
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

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public record CreatePaymentIntentCommand(UUID bookingId, String externalReference) {
    }

    public record AmountCommand(Long amountMinor, String externalReference) {
    }
}
