package com.kayledger.api.subscription.application;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.payment.model.PaymentIntent;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;
import com.kayledger.api.subscription.model.EntitlementGrant;
import com.kayledger.api.subscription.model.SubscriptionCycle;
import com.kayledger.api.subscription.model.SubscriptionPlan;
import com.kayledger.api.subscription.model.SubscriptionPlanChange;
import com.kayledger.api.subscription.model.SubscriptionRecord;
import com.kayledger.api.subscription.store.SubscriptionStore;

@Service
public class SubscriptionService {

    private static final Set<String> BILLING_INTERVALS = Set.of("MONTHLY", "YEARLY");

    private final SubscriptionStore subscriptionStore;
    private final PaymentStore paymentStore;
    private final AccessPolicy accessPolicy;

    public SubscriptionService(SubscriptionStore subscriptionStore, PaymentStore paymentStore, AccessPolicy accessPolicy) {
        this.subscriptionStore = subscriptionStore;
        this.paymentStore = paymentStore;
        this.accessPolicy = accessPolicy;
    }

    @Transactional
    public SubscriptionPlan createPlan(AccessContext context, CreatePlanCommand command) {
        requireSubscriptionWrite(context);
        return subscriptionStore.createPlan(
                context.workspaceId(),
                requireId(command.providerProfileId(), "providerProfileId"),
                requireText(command.planCode(), "planCode"),
                requireText(command.displayName(), "displayName"),
                requireOneOf(command.billingInterval(), BILLING_INTERVALS, "billingInterval"),
                requireCurrency(command.currencyCode()),
                requirePositive(command.amountMinor(), "amountMinor"));
    }

    public List<SubscriptionPlan> listPlans(AccessContext context) {
        requireSubscriptionRead(context);
        return subscriptionStore.listPlans(context.workspaceId());
    }

    @Transactional
    public SubscriptionRecord createSubscription(AccessContext context, CreateSubscriptionCommand command) {
        requireSubscriptionWrite(context);
        SubscriptionPlan plan = activePlan(context.workspaceId(), requireId(command.planId(), "planId"));
        Instant startAt = command.startAt() == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : command.startAt();
        Instant endAt = periodEnd(startAt, plan.billingInterval());
        SubscriptionRecord subscription = subscriptionStore.createSubscription(
                context.workspaceId(),
                requireId(command.customerProfileId(), "customerProfileId"),
                plan,
                startAt,
                endAt);
        SubscriptionCycle cycle = subscriptionStore.createCycle(context.workspaceId(), subscription.id(), 1, plan, subscription.customerProfileId(), startAt, endAt, "PENDING_PAYMENT", "initial-cycle");
        PaymentIntent intent = paymentStore.createSubscriptionIntent(
                context.workspaceId(),
                subscription.id(),
                cycle.id(),
                plan.providerProfileId(),
                cycle.currencyCode(),
                cycle.grossAmountMinor(),
                cycle.feeAmountMinor(),
                cycle.netAmountMinor(),
                "subscription-cycle-" + cycle.id());
        subscriptionStore.attachPaymentIntent(context.workspaceId(), cycle.id(), intent.id());
        return subscription;
    }

    public List<SubscriptionRecord> listSubscriptions(AccessContext context) {
        requireSubscriptionRead(context);
        return subscriptionStore.listSubscriptions(context.workspaceId());
    }

    public List<SubscriptionCycle> listCycles(AccessContext context, UUID subscriptionId) {
        requireSubscriptionRead(context);
        SubscriptionRecord subscription = subscription(context, subscriptionId);
        return subscriptionStore.listCycles(context.workspaceId(), subscription.id());
    }

    public List<EntitlementGrant> listEntitlements(AccessContext context, UUID subscriptionId) {
        requireSubscriptionRead(context);
        SubscriptionRecord subscription = subscription(context, subscriptionId);
        return subscriptionStore.listEntitlements(context.workspaceId(), subscription.id());
    }

    @Transactional
    public SubscriptionRecord cancel(AccessContext context, UUID subscriptionId) {
        requireSubscriptionWrite(context);
        SubscriptionRecord subscription = subscription(context, subscriptionId);
        SubscriptionRecord cancelled = subscriptionStore.cancel(context.workspaceId(), subscription.id(), Instant.now().truncatedTo(ChronoUnit.SECONDS));
        subscriptionStore.upsertEntitlement(context.workspaceId(), cancelled.id(), cancelled.customerProfileId(), "CANCELLED", cancelled.startAt(), cancelled.cancelledAt());
        return cancelled;
    }

    @Transactional
    public SubscriptionPlanChange schedulePlanChange(AccessContext context, UUID subscriptionId, SchedulePlanChangeCommand command) {
        requireSubscriptionWrite(context);
        SubscriptionRecord subscription = subscription(context, subscriptionId);
        SubscriptionPlan target = activePlan(context.workspaceId(), requireId(command.targetPlanId(), "targetPlanId"));
        int effectiveCycleNumber = command.effectiveCycleNumber() == null
                ? subscriptionStore.nextCycleNumber(context.workspaceId(), subscription.id())
                : command.effectiveCycleNumber();
        if (effectiveCycleNumber <= subscriptionStore.nextCycleNumber(context.workspaceId(), subscription.id()) - 1) {
            throw new BadRequestException("effectiveCycleNumber must be a future cycle.");
        }
        return subscriptionStore.schedulePlanChange(context.workspaceId(), subscription.id(), target.id(), effectiveCycleNumber);
    }

    @Transactional
    public RenewalRunResult runDueRenewals(AccessContext context, RenewalRunCommand command) {
        requireSubscriptionRenew(context);
        Instant now = command == null || command.now() == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : command.now();
        boolean forceFailure = command != null && Boolean.TRUE.equals(command.forceFailure());
        int processed = 0;
        int paymentIntentsCreated = 0;
        for (SubscriptionRecord subscription : subscriptionStore.dueSubscriptions(context.workspaceId(), now)) {
            Instant startAt = subscription.currentPeriodEndAt();
            SubscriptionCycle cycle = subscriptionStore.findCycleForPeriod(context.workspaceId(), subscription.id(), startAt)
                    .orElseGet(() -> {
                        int cycleNumber = subscriptionStore.nextCycleNumber(context.workspaceId(), subscription.id());
                        SubscriptionPlan plan = planForCycle(context.workspaceId(), subscription, cycleNumber);
                        Instant endAt = periodEnd(startAt, plan.billingInterval());
                        return subscriptionStore.createCycle(
                                context.workspaceId(),
                                subscription.id(),
                                cycleNumber,
                                plan,
                                subscription.customerProfileId(),
                                startAt,
                                endAt,
                                forceFailure ? "FAILED" : "PENDING_PAYMENT",
                                "renewal-" + cycleNumber);
                    });
            if (forceFailure) {
                SubscriptionRecord grace = subscriptionStore.moveToGrace(context.workspaceId(), subscription.id(), now.plus(7, ChronoUnit.DAYS));
                subscriptionStore.upsertEntitlement(context.workspaceId(), grace.id(), grace.customerProfileId(), "GRACE", grace.currentPeriodStartAt(), grace.graceExpiresAt());
            } else if (cycle.paymentIntentId() == null) {
                PaymentIntent intent = paymentStore.createSubscriptionIntent(
                        context.workspaceId(),
                        subscription.id(),
                        cycle.id(),
                        cycle.providerProfileId(),
                        cycle.currencyCode(),
                        cycle.grossAmountMinor(),
                        cycle.feeAmountMinor(),
                        cycle.netAmountMinor(),
                        "subscription-cycle-" + cycle.id());
                subscriptionStore.attachPaymentIntent(context.workspaceId(), cycle.id(), intent.id());
                paymentIntentsCreated++;
            }
            processed++;
        }
        return new RenewalRunResult(processed, paymentIntentsCreated);
    }

    @Transactional
    public SuspensionRunResult transitionGraceToSuspension(AccessContext context, SuspensionRunCommand command) {
        requireSubscriptionRenew(context);
        Instant now = command == null || command.now() == null ? Instant.now().truncatedTo(ChronoUnit.SECONDS) : command.now();
        int suspended = 0;
        for (SubscriptionRecord subscription : subscriptionStore.graceExpiredSubscriptions(context.workspaceId(), now)) {
            SubscriptionRecord updated = subscriptionStore.moveToSuspended(context.workspaceId(), subscription.id());
            subscriptionStore.upsertEntitlement(context.workspaceId(), updated.id(), updated.customerProfileId(), "SUSPENDED", updated.startAt(), now);
            suspended++;
        }
        return new SuspensionRunResult(suspended);
    }

    private SubscriptionPlan planForCycle(UUID workspaceId, SubscriptionRecord subscription, int cycleNumber) {
        SubscriptionPlan plan = activePlan(workspaceId, subscription.currentPlanId());
        var change = subscriptionStore.pendingPlanChange(workspaceId, subscription.id(), cycleNumber, subscription.currentPeriodEndAt());
        if (change.isPresent()) {
            SubscriptionPlan target = activePlan(workspaceId, change.get().targetPlanId());
            return target;
        }
        return plan;
    }

    private SubscriptionPlan activePlan(UUID workspaceId, UUID planId) {
        SubscriptionPlan plan = subscriptionStore.findPlan(workspaceId, planId)
                .orElseThrow(() -> new NotFoundException("Subscription plan was not found."));
        if (!"ACTIVE".equals(plan.status())) {
            throw new BadRequestException("Only active subscription plans can be used.");
        }
        return plan;
    }

    private SubscriptionRecord subscription(AccessContext context, UUID subscriptionId) {
        return subscriptionStore.findSubscription(context.workspaceId(), requireId(subscriptionId, "subscriptionId"))
                .orElseThrow(() -> new NotFoundException("Subscription was not found."));
    }

    private void requireSubscriptionRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireSubscriptionWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private void requireSubscriptionRenew(AccessContext context) {
        requireSubscriptionWrite(context);
    }

    private static Instant periodEnd(Instant startAt, String billingInterval) {
        if ("YEARLY".equals(billingInterval)) {
            return startAt.plus(365, ChronoUnit.DAYS);
        }
        return startAt.plus(30, ChronoUnit.DAYS);
    }

    private static UUID requireId(UUID value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static long requirePositive(Long value, String field) {
        if (value == null || value <= 0) {
            throw new BadRequestException(field + " must be greater than zero.");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireCurrency(String value) {
        String currency = requireText(value, "currencyCode").toUpperCase();
        if (!currency.matches("[A-Z]{3}")) {
            throw new BadRequestException("currencyCode must be a three-letter ISO code.");
        }
        return currency;
    }

    private static String requireOneOf(String value, Set<String> allowed, String field) {
        String required = requireText(value, field);
        if (!allowed.contains(required)) {
            throw new BadRequestException(field + " is invalid.");
        }
        return required;
    }

    public record CreatePlanCommand(UUID providerProfileId, String planCode, String displayName, String billingInterval, String currencyCode, Long amountMinor) {
    }

    public record CreateSubscriptionCommand(UUID customerProfileId, UUID planId, Instant startAt) {
    }

    public record SchedulePlanChangeCommand(UUID targetPlanId, Integer effectiveCycleNumber) {
    }

    public record RenewalRunCommand(Instant now, Boolean forceFailure) {
    }

    public record SuspensionRunCommand(Instant now) {
    }

    public record RenewalRunResult(int subscriptionsProcessed, int paymentIntentsCreated) {
    }

    public record SuspensionRunResult(int subscriptionsSuspended) {
    }
}
