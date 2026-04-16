package com.kayledger.api.booking.api;

import java.util.List;
import java.util.UUID;

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
import com.kayledger.api.booking.model.BookingDetails;

@RestController
@RequestMapping("/api/bookings")
public class BookingsController {

    private final BookingService bookingService;
    private final AccessContextResolver accessContextResolver;

    public BookingsController(
            BookingService bookingService,
            AccessContextResolver accessContextResolver) {
        this.bookingService = bookingService;
        this.accessContextResolver = accessContextResolver;
    }

    @PostMapping
    BookingDetails createHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody CreateBookingCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return bookingService.createHeld(context, request);
    }

    @GetMapping
    List<Booking> list(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return bookingService.list(context);
    }

    @PostMapping("/{bookingId}/cancel")
    BookingDetails cancelHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID bookingId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return bookingService.cancelHeld(context, bookingId);
    }

    @PostMapping("/{bookingId}/expire")
    BookingDetails expireHeld(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID bookingId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return bookingService.expireHeld(context, bookingId);
    }
}
