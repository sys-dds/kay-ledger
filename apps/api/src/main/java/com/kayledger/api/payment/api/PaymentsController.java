package com.kayledger.api.payment.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.payment.application.PaymentService.AmountCommand;
import com.kayledger.api.payment.application.PaymentService.CreatePaymentIntentCommand;
import com.kayledger.api.payment.application.PaymentService.DisputeCommand;
import com.kayledger.api.payment.application.PaymentService.PayoutMutationCommand;
import com.kayledger.api.payment.application.PaymentService.PayoutRequestCommand;
import com.kayledger.api.payment.application.PaymentService.RefundCommand;
import com.kayledger.api.payment.application.PaymentService.RefundFailureCommand;
import com.kayledger.api.payment.application.PaymentService.ResolveDisputeCommand;
import com.kayledger.api.payment.model.DisputeRecord;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.PaymentIntentDetails;
import com.kayledger.api.payment.model.PayoutAttempt;
import com.kayledger.api.payment.model.PayoutRequest;
import com.kayledger.api.payment.model.RefundRecord;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/payments")
public class PaymentsController {

    private final PaymentService paymentService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;

    public PaymentsController(
            PaymentService paymentService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService) {
        this.paymentService = paymentService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/intents")
    ResponseEntity<Object> createIntent(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreatePaymentIntentCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.createIntent(context, request));
    }

    @GetMapping("/intents")
    List<PaymentIntent> listIntents(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.list(context);
    }

    @GetMapping("/intents/by-booking/{bookingId}")
    PaymentIntentDetails byBooking(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID bookingId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.findByBooking(context, bookingId);
    }

    @PostMapping("/intents/{paymentIntentId}/authorize")
    ResponseEntity<Object> authorize(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/authorize",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.authorize(context, paymentIntentId, request));
    }

    @PostMapping("/intents/{paymentIntentId}/capture")
    ResponseEntity<Object> capture(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/capture",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.capture(context, paymentIntentId, request));
    }

    @PostMapping("/intents/{paymentIntentId}/cancel")
    ResponseEntity<Object> cancel(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/cancel",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.cancel(context, paymentIntentId, request));
    }

    @PostMapping("/intents/{paymentIntentId}/requires-action")
    ResponseEntity<Object> requireAction(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/requires-action",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.requireAction(context, paymentIntentId, request));
    }

    @PostMapping("/intents/{paymentIntentId}/fail")
    ResponseEntity<Object> fail(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/fail",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.fail(context, paymentIntentId, request));
    }

    @PostMapping("/intents/{paymentIntentId}/settle")
    ResponseEntity<Object> settle(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID paymentIntentId,
            @RequestBody(required = false) AmountCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/intents/{paymentIntentId}/settle",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, paymentIntentId, request),
                () -> paymentService.settle(context, paymentIntentId, request));
    }

    @PostMapping("/payouts")
    ResponseEntity<Object> requestPayout(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody PayoutRequestCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/payouts",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.requestPayout(context, request));
    }

    @GetMapping("/payouts")
    List<PayoutRequest> listPayouts(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.listPayouts(context);
    }

    @GetMapping("/payouts/{payoutRequestId}/attempts")
    List<PayoutAttempt> payoutAttempts(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID payoutRequestId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.listPayoutAttempts(context, payoutRequestId);
    }

    @PostMapping("/payouts/{payoutRequestId}/retry")
    ResponseEntity<Object> retryPayout(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID payoutRequestId,
            @RequestBody(required = false) PayoutMutationCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/payouts/{payoutRequestId}/retry",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, payoutRequestId, request),
                () -> paymentService.retryPayout(context, payoutRequestId, request));
    }

    @PostMapping("/payouts/{payoutRequestId}/succeed")
    ResponseEntity<Object> markPayoutSucceeded(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID payoutRequestId,
            @RequestBody(required = false) PayoutMutationCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/payouts/{payoutRequestId}/succeed",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, payoutRequestId, request),
                () -> paymentService.markPayoutSucceeded(context, payoutRequestId, request));
    }

    @PostMapping("/payouts/{payoutRequestId}/fail")
    ResponseEntity<Object> markPayoutFailed(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID payoutRequestId,
            @RequestBody(required = false) PayoutMutationCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/payouts/{payoutRequestId}/fail",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, payoutRequestId, request),
                () -> paymentService.markPayoutFailed(context, payoutRequestId, request));
    }

    @PostMapping("/refunds/full")
    ResponseEntity<Object> createFullRefund(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody RefundCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/refunds/full",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.createFullRefund(context, request));
    }

    @PostMapping("/refunds/partial")
    ResponseEntity<Object> createPartialRefund(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody RefundCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/refunds/partial",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.createPartialRefund(context, request));
    }

    @PostMapping("/reversals")
    ResponseEntity<Object> createReversal(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody RefundCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/reversals",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.createReversal(context, request));
    }

    @GetMapping("/refunds")
    List<RefundRecord> listRefunds(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.listRefunds(context);
    }

    @PostMapping("/refunds/{refundId}/fail")
    ResponseEntity<Object> markRefundFailed(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID refundId,
            @RequestBody RefundFailureCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/refunds/{refundId}/fail",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, refundId, request),
                () -> paymentService.markRefundFailed(context, refundId, request));
    }

    @PostMapping("/refunds/{refundId}/retry")
    ResponseEntity<Object> retryRefund(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID refundId,
            @RequestBody(required = false) RefundFailureCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/refunds/{refundId}/retry",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, refundId, request),
                () -> paymentService.retryRefund(context, refundId, request));
    }

    @PostMapping("/disputes")
    ResponseEntity<Object> openDispute(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody DisputeCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/disputes",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> paymentService.openDispute(context, request));
    }

    @PostMapping("/disputes/{disputeId}/resolve")
    ResponseEntity<Object> resolveDispute(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID disputeId,
            @RequestBody ResolveDisputeCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/payments/disputes/{disputeId}/resolve",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, disputeId, request),
                () -> paymentService.resolveDispute(context, disputeId, request));
    }

    @GetMapping("/disputes")
    List<DisputeRecord> listDisputes(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return paymentService.listDisputes(context);
    }
}
