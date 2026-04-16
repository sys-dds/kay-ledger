package com.kayledger.api.booking.application;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.booking.model.Booking;
import com.kayledger.api.booking.model.BookingDetails;
import com.kayledger.api.booking.model.BookingHold;
import com.kayledger.api.booking.store.BookingStore;
import com.kayledger.api.catalog.Offering;
import com.kayledger.api.catalog.OfferingAvailabilityWindow;
import com.kayledger.api.catalog.OfferingStore;
import com.kayledger.api.identity.CustomerProfile;
import com.kayledger.api.identity.ProfileStore;
import com.kayledger.api.identity.ProviderProfile;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class BookingService {

    private static final String SCHEDULED_TIME = "SCHEDULED_TIME";
    private static final String QUANTITY = "QUANTITY";
    private static final String PUBLISHED = "PUBLISHED";
    private static final String ACTIVE = "ACTIVE";

    private final BookingStore bookingStore;
    private final OfferingStore offeringStore;
    private final ProfileStore profileStore;
    private final AccessPolicy accessPolicy;

    public BookingService(
            BookingStore bookingStore,
            OfferingStore offeringStore,
            ProfileStore profileStore,
            AccessPolicy accessPolicy) {
        this.bookingStore = bookingStore;
        this.offeringStore = offeringStore;
        this.profileStore = profileStore;
        this.accessPolicy = accessPolicy;
    }

    @Transactional
    public BookingDetails createHeld(AccessContext context, CreateBookingCommand command) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.CUSTOMER);
        accessPolicy.requireScope(context, AccessScope.BOOKING_CREATE);
        Instant now = Instant.now();
        Offering offering = offeringStore.findForWorkspaceForUpdate(context.workspaceId(), requireId(command.offeringId(), "offeringId"))
                .orElseThrow(() -> new NotFoundException("Offering was not found."));
        if (!PUBLISHED.equals(offering.status())) {
            throw new BadRequestException("Only published offerings can be booked.");
        }
        CustomerProfile customerProfile = customerProfile(context, command.customerProfileId());
        enforceCustomerCreationBoundary(context, customerProfile);
        int holdTtlSeconds = command.holdTtlSeconds() == null ? 900 : command.holdTtlSeconds();
        if (holdTtlSeconds < 0) {
            throw new BadRequestException("holdTtlSeconds must be zero or greater.");
        }
        Instant holdExpiresAt = now.plusSeconds(holdTtlSeconds);

        try {
            Booking booking = switch (offering.offerType()) {
                case SCHEDULED_TIME -> createScheduledHold(context, offering, customerProfile, command, holdExpiresAt, now);
                case QUANTITY -> createQuantityHold(context, offering, customerProfile, command, holdExpiresAt, now);
                default -> throw new BadRequestException("Offering type is not bookable.");
            };
            BookingHold hold = bookingStore.createHold(context.workspaceId(), booking.id(), holdExpiresAt);
            return new BookingDetails(booking, hold);
        } catch (DataIntegrityViolationException exception) {
            throw new BadRequestException("Requested booking conflicts with an active hold or booking.");
        }
    }

    public List<Booking> list(AccessContext context) {
        accessPolicy.requireScope(context, AccessScope.BOOKING_READ);
        if (WorkspaceRole.OWNER.equals(context.workspaceRole()) || WorkspaceRole.ADMIN.equals(context.workspaceRole())) {
            return bookingStore.listForWorkspace(context.workspaceId());
        }
        if (WorkspaceRole.PROVIDER.equals(context.workspaceRole())) {
            List<UUID> providerProfileIds = profileStore.listProviders(context.workspaceId()).stream()
                    .filter(profile -> ACTIVE.equals(profile.status()))
                    .filter(profile -> profile.actorId().equals(context.actorId()))
                    .map(ProviderProfile::id)
                    .toList();
            return bookingStore.listForProvider(context.workspaceId(), providerProfileIds);
        }
        if (WorkspaceRole.CUSTOMER.equals(context.workspaceRole())) {
            List<UUID> customerProfileIds = profileStore.listCustomers(context.workspaceId()).stream()
                    .filter(profile -> ACTIVE.equals(profile.status()))
                    .filter(profile -> profile.actorId().equals(context.actorId()))
                    .map(CustomerProfile::id)
                    .toList();
            return bookingStore.listForCustomer(context.workspaceId(), customerProfileIds);
        }
        throw new ForbiddenException("Actor cannot inspect bookings.");
    }

    @Transactional
    public BookingDetails cancelHeld(AccessContext context, UUID bookingId) {
        Booking booking = booking(context, bookingId);
        requireCancelable(context, booking);
        Booking cancelled = bookingStore.cancelHeld(context.workspaceId(), booking.id(), Instant.now())
                .orElseThrow(() -> new BadRequestException("Only held bookings can be cancelled."));
        return details(context.workspaceId(), cancelled);
    }

    @Transactional
    public BookingDetails expireHeld(AccessContext context, UUID bookingId) {
        Booking booking = booking(context, bookingId);
        requireVisible(context, booking);
        if (booking.holdExpiresAt().isAfter(Instant.now())) {
            throw new BadRequestException("Hold has not expired yet.");
        }
        Booking expired = bookingStore.expireHeld(context.workspaceId(), booking.id(), Instant.now())
                .orElseThrow(() -> new BadRequestException("Only expired held bookings can be released."));
        return details(context.workspaceId(), expired);
    }

    private Booking createScheduledHold(
            AccessContext context,
            Offering offering,
            CustomerProfile customerProfile,
            CreateBookingCommand command,
            Instant holdExpiresAt,
            Instant now) {
        if (command.quantity() != null || command.scheduledStartAt() == null || command.scheduledEndAt() == null) {
            throw new BadRequestException("Scheduled-time bookings require scheduledStartAt and scheduledEndAt only.");
        }
        Instant scheduledStartAt = command.scheduledStartAt();
        Instant scheduledEndAt = command.scheduledEndAt();
        if (!scheduledEndAt.equals(scheduledStartAt.plus(Duration.ofMinutes(offering.durationMinutes())))) {
            throw new BadRequestException("Scheduled booking duration must match the offering duration.");
        }
        requireNotice(offering, scheduledStartAt, now);
        requireSlotAlignment(offering, scheduledStartAt);
        requireAvailability(context.workspaceId(), offering, scheduledStartAt, scheduledEndAt);
        return bookingStore.createHeld(
                context.workspaceId(),
                offering.id(),
                offering.providerProfileId(),
                customerProfile.id(),
                SCHEDULED_TIME,
                scheduledStartAt,
                scheduledEndAt,
                1,
                holdExpiresAt);
    }

    private Booking createQuantityHold(
            AccessContext context,
            Offering offering,
            CustomerProfile customerProfile,
            CreateBookingCommand command,
            Instant holdExpiresAt,
            Instant now) {
        if (command.scheduledStartAt() != null || command.scheduledEndAt() != null) {
            throw new BadRequestException("Quantity bookings must omit scheduled-time fields.");
        }
        int quantity = requirePositive(command.quantity(), "quantity");
        bookingStore.expireHeldForOffering(context.workspaceId(), offering.id(), now);
        int activeQuantity = bookingStore.activeQuantityReserved(context.workspaceId(), offering.id(), now);
        if (activeQuantity + quantity > offering.quantityAvailable()) {
            throw new BadRequestException("Requested quantity exceeds available quantity.");
        }
        return bookingStore.createHeld(
                context.workspaceId(),
                offering.id(),
                offering.providerProfileId(),
                customerProfile.id(),
                QUANTITY,
                null,
                null,
                quantity,
                holdExpiresAt);
    }

    private void requireNotice(Offering offering, Instant scheduledStartAt, Instant now) {
        long minutesUntilStart = Duration.between(now, scheduledStartAt).toMinutes();
        if (minutesUntilStart < offering.minNoticeMinutes()) {
            throw new BadRequestException("Scheduled booking violates minimum notice.");
        }
        if (offering.maxNoticeDays() != null && scheduledStartAt.isAfter(now.plus(Duration.ofDays(offering.maxNoticeDays())))) {
            throw new BadRequestException("Scheduled booking violates maximum notice.");
        }
    }

    private void requireSlotAlignment(Offering offering, Instant scheduledStartAt) {
        LocalTime startTime = LocalDateTime.ofInstant(scheduledStartAt, ZoneOffset.UTC).toLocalTime();
        int minutesFromMidnight = startTime.getHour() * 60 + startTime.getMinute();
        if (minutesFromMidnight % offering.slotIntervalMinutes() != 0) {
            throw new BadRequestException("Scheduled booking is not aligned to the offering slot interval.");
        }
    }

    private void requireAvailability(UUID workspaceId, Offering offering, Instant scheduledStartAt, Instant scheduledEndAt) {
        LocalDateTime start = LocalDateTime.ofInstant(scheduledStartAt, ZoneOffset.UTC);
        LocalDateTime end = LocalDateTime.ofInstant(scheduledEndAt, ZoneOffset.UTC);
        if (!start.toLocalDate().equals(end.toLocalDate())) {
            throw new BadRequestException("Scheduled booking must fit inside one availability window.");
        }
        LocalDate date = start.toLocalDate();
        int weekday = start.getDayOfWeek().getValue();
        boolean fits = offeringStore.listAvailabilityWindows(workspaceId, offering.id()).stream()
                .filter(window -> window.weekday() == weekday)
                .filter(window -> window.effectiveFrom() == null || !date.isBefore(window.effectiveFrom()))
                .filter(window -> window.effectiveTo() == null || !date.isAfter(window.effectiveTo()))
                .anyMatch(window -> fitsWindow(window, start.toLocalTime(), end.toLocalTime()));
        if (!fits) {
            throw new BadRequestException("Scheduled booking does not fit an active availability window.");
        }
    }

    private boolean fitsWindow(OfferingAvailabilityWindow window, LocalTime start, LocalTime end) {
        return !start.isBefore(window.startLocalTime()) && !end.isAfter(window.endLocalTime());
    }

    private CustomerProfile customerProfile(AccessContext context, UUID customerProfileId) {
        CustomerProfile customerProfile = profileStore.findCustomer(context.workspaceId(), requireId(customerProfileId, "customerProfileId"))
                .orElseThrow(() -> new BadRequestException("customerProfileId is not valid for this workspace."));
        if (!ACTIVE.equals(customerProfile.status())) {
            throw new BadRequestException("customerProfileId does not identify an active customer profile.");
        }
        return customerProfile;
    }

    private void enforceCustomerCreationBoundary(AccessContext context, CustomerProfile customerProfile) {
        if (WorkspaceRole.CUSTOMER.equals(context.workspaceRole()) && !customerProfile.actorId().equals(context.actorId())) {
            throw new ForbiddenException("Customers can only create bookings for themselves.");
        }
    }

    private Booking booking(AccessContext context, UUID bookingId) {
        return bookingStore.find(context.workspaceId(), requireId(bookingId, "bookingId"))
                .orElseThrow(() -> new NotFoundException("Booking was not found."));
    }

    private BookingDetails details(UUID workspaceId, Booking booking) {
        return new BookingDetails(booking, bookingStore.holdForBooking(workspaceId, booking.id()));
    }

    private void requireCancelable(AccessContext context, Booking booking) {
        if (WorkspaceRole.OWNER.equals(context.workspaceRole()) || WorkspaceRole.ADMIN.equals(context.workspaceRole())) {
            accessPolicy.requireScope(context, AccessScope.BOOKING_MANAGE);
            return;
        }
        if (WorkspaceRole.CUSTOMER.equals(context.workspaceRole())) {
            accessPolicy.requireScope(context, AccessScope.BOOKING_CREATE);
            CustomerProfile customerProfile = profileStore.findCustomer(context.workspaceId(), booking.customerProfileId())
                    .orElseThrow(() -> new NotFoundException("Booking customer profile was not found."));
            if (customerProfile.actorId().equals(context.actorId())) {
                return;
            }
        }
        throw new ForbiddenException("Actor cannot cancel this booking.");
    }

    private void requireVisible(AccessContext context, Booking booking) {
        accessPolicy.requireScope(context, AccessScope.BOOKING_READ);
        if (WorkspaceRole.OWNER.equals(context.workspaceRole()) || WorkspaceRole.ADMIN.equals(context.workspaceRole())) {
            return;
        }
        if (WorkspaceRole.PROVIDER.equals(context.workspaceRole())) {
            boolean ownsProviderProfile = profileStore.listProviders(context.workspaceId()).stream()
                    .anyMatch(profile -> ACTIVE.equals(profile.status())
                            && profile.id().equals(booking.providerProfileId())
                            && profile.actorId().equals(context.actorId()));
            if (ownsProviderProfile) {
                return;
            }
        }
        if (WorkspaceRole.CUSTOMER.equals(context.workspaceRole())) {
            boolean ownsCustomerProfile = profileStore.findCustomer(context.workspaceId(), booking.customerProfileId())
                    .map(profile -> ACTIVE.equals(profile.status()) && profile.actorId().equals(context.actorId()))
                    .orElse(false);
            if (ownsCustomerProfile) {
                return;
            }
        }
        throw new ForbiddenException("Actor cannot inspect this booking.");
    }

    private static UUID requireId(UUID value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static int requirePositive(Integer value, String field) {
        if (value == null || value <= 0) {
            throw new BadRequestException(field + " must be greater than zero.");
        }
        return value;
    }

    public record CreateBookingCommand(
            UUID offeringId,
            UUID customerProfileId,
            Instant scheduledStartAt,
            Instant scheduledEndAt,
            Integer quantity,
            Integer holdTtlSeconds) {
    }
}
