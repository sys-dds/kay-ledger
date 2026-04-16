package com.kayledger.api.booking.api;

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
import com.kayledger.api.booking.application.BookingService;
import com.kayledger.api.booking.application.BookingService.CreateBookingCommand;
import com.kayledger.api.booking.model.Booking;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/bookings")
public class BookingsController {

    private final BookingService bookingService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;

    public BookingsController(
            BookingService bookingService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService) {
        this.bookingService = bookingService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    ResponseEntity<Object> createHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateBookingCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/bookings",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> bookingService.createHeld(context, request));
    }

    @GetMapping
    List<Booking> list(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return bookingService.list(context);
    }

    @PostMapping("/{bookingId}/cancel")
    ResponseEntity<Object> cancelHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID bookingId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/bookings/{bookingId}/cancel",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, bookingId),
                () -> bookingService.cancelHeld(context, bookingId));
    }

    @PostMapping("/{bookingId}/expire")
    ResponseEntity<Object> expireHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID bookingId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/bookings/{bookingId}/expire",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, bookingId),
                () -> bookingService.expireHeld(context, bookingId));
    }
}
