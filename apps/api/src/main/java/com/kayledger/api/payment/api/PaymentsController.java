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
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.model.PaymentIntentDetails;
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
}
