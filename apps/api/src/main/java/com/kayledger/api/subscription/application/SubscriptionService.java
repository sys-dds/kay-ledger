package com.kayledger.api.subscription.application;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import com.kayledger.api.shared.messaging.application.OutboxService;
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
    private final OutboxService outboxService;

    public SubscriptionService(SubscriptionStore subscriptionStore, PaymentStore paymentStore, AccessPolicy accessPolicy, OutboxService outboxService) {
        this.subscriptionStore = subscriptionStore;
        this.paymentStore = paymentStore;
        this.accessPolicy = accessPolicy;
        this.outboxService = outboxService;
    }

    @Transactional
    public SubscriptionPlan createPlan(AccessContext context, CreatePlanCommand command) {
        requireSubscriptionWrite(context);
        SubscriptionPlan plan = subscriptionStore.createPlan(
                context.workspaceId(),
                requireId(command.providerProfileId(), "providerProfileId"),
                requireText(command.planCode(), "planCode"),
                requireText(command.displayName(), "displayName"),
                requireOneOf(command.billingInterval(), BILLING_INTERVALS, "billingInterval"),
                requireCurrency(command.currencyCode()),
                requirePositive(command.amountMinor(), "amountMinor"));
        outboxService.append(context.workspaceId(), "SUBSCRIPTION_PLAN", plan.id(), "subscription.plan.created", "subscription.plan.created:" + plan.id(), planData(plan));
        return plan;
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
        outboxService.append(context.workspaceId(), "SUBSCRIPTION", subscription.id(), "subscription.created", "subscription.created:" + subscription.id(), subscriptionData(subscription));
        outboxService.append(context.workspaceId(), "SUBSCRIPTION_CYCLE", cycle.id(), "subscription.cycle.created", "subscription.cycle.created:" + cycle.id(), cycleData(cycle, intent.id()));
        outboxService.append(context.workspaceId(), "PAYMENT", intent.id(), "payment.intent.created", "payment.intent.created:" + intent.id() + ":" + intent.status(), paymentData(intent));
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
        SubscriptionPlanChange change = subscriptionStore.schedulePlanChange(context.workspaceId(), subscription.id(), target.id(), effectiveCycleNumber);
        outboxService.append(context.workspaceId(), "SUBSCRIPTION", subscription.id(), "subscription.plan_change.scheduled", "subscription.plan_change.scheduled:" + change.id(), planChangeData(change));
        return change;
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
                        SubscriptionCycle created = subscriptionStore.createCycle(
                                context.workspaceId(),
                                subscription.id(),
                                cycleNumber,
                                plan,
                                subscription.customerProfileId(),
                                startAt,
                                endAt,
                                forceFailure ? "FAILED" : "PENDING_PAYMENT",
                                "renewal-" + cycleNumber);
                        outboxService.append(context.workspaceId(), "SUBSCRIPTION_CYCLE", created.id(), "subscription.cycle.created", "subscription.cycle.created:" + created.id(), cycleData(created, null));
                        return created;
                    });
            if (forceFailure) {
                SubscriptionRecord grace = subscriptionStore.moveToGrace(context.workspaceId(), subscription.id(), now.plus(7, ChronoUnit.DAYS));
                subscriptionStore.upsertEntitlement(context.workspaceId(), grace.id(), grace.customerProfileId(), "GRACE", grace.currentPeriodStartAt(), grace.graceExpiresAt());
                outboxService.append(context.workspaceId(), "SUBSCRIPTION", grace.id(), "subscription.renewal.failed_to_grace", "subscription.renewal.failed_to_grace:" + grace.id() + ":" + cycle.id(), subscriptionData(grace));
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
                outboxService.append(context.workspaceId(), "PAYMENT", intent.id(), "payment.intent.created", "payment.intent.created:" + intent.id() + ":" + intent.status(), paymentData(intent));
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
            outboxService.append(context.workspaceId(), "SUBSCRIPTION", updated.id(), "subscription.suspended", "subscription.suspended:" + updated.id(), subscriptionData(updated));
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

    private Map<String, Object> planData(SubscriptionPlan plan) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("planId", plan.id());
        data.put("providerProfileId", plan.providerProfileId());
        data.put("planCode", plan.planCode());
        data.put("billingInterval", plan.billingInterval());
        data.put("currencyCode", plan.currencyCode());
        data.put("amountMinor", plan.amountMinor());
        data.put("status", plan.status());
        return data;
    }

    private Map<String, Object> subscriptionData(SubscriptionRecord subscription) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("subscriptionId", subscription.id());
        data.put("customerProfileId", subscription.customerProfileId());
        data.put("currentPlanId", subscription.currentPlanId());
        data.put("providerProfileId", subscription.providerProfileId());
        data.put("status", subscription.status());
        data.put("currentPeriodStartAt", subscription.currentPeriodStartAt());
        data.put("currentPeriodEndAt", subscription.currentPeriodEndAt());
        if (subscription.graceExpiresAt() != null) {
            data.put("graceExpiresAt", subscription.graceExpiresAt());
        }
        return data;
    }

    private Map<String, Object> cycleData(SubscriptionCycle cycle, UUID paymentIntentId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("subscriptionCycleId", cycle.id());
        data.put("subscriptionId", cycle.subscriptionId());
        data.put("cycleNumber", cycle.cycleNumber());
        data.put("planId", cycle.planId());
        data.put("providerProfileId", cycle.providerProfileId());
        data.put("customerProfileId", cycle.customerProfileId());
        data.put("cycleStartAt", cycle.cycleStartAt());
        data.put("cycleEndAt", cycle.cycleEndAt());
        data.put("status", cycle.status());
        data.put("currencyCode", cycle.currencyCode());
        data.put("grossAmountMinor", cycle.grossAmountMinor());
        data.put("feeAmountMinor", cycle.feeAmountMinor());
        data.put("netAmountMinor", cycle.netAmountMinor());
        if (paymentIntentId != null) {
            data.put("paymentIntentId", paymentIntentId);
        }
        return data;
    }

    private Map<String, Object> planChangeData(SubscriptionPlanChange change) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("planChangeId", change.id());
        data.put("subscriptionId", change.subscriptionId());
        data.put("targetPlanId", change.targetPlanId());
        data.put("effectiveCycleNumber", change.effectiveCycleNumber());
        data.put("status", change.status());
        return data;
    }

    private Map<String, Object> paymentData(PaymentIntent intent) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("paymentIntentId", intent.id());
        if (intent.bookingId() != null) {
            data.put("bookingId", intent.bookingId());
        }
        if (intent.subscriptionId() != null) {
            data.put("subscriptionId", intent.subscriptionId());
        }
        if (intent.subscriptionCycleId() != null) {
            data.put("subscriptionCycleId", intent.subscriptionCycleId());
        }
        data.put("providerProfileId", intent.providerProfileId());
        data.put("status", intent.status());
        data.put("currencyCode", intent.currencyCode());
        data.put("grossAmountMinor", intent.grossAmountMinor());
        data.put("feeAmountMinor", intent.feeAmountMinor());
        data.put("netAmountMinor", intent.netAmountMinor());
        return data;
    }

    private void requireSubscriptionRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.SUBSCRIPTION_READ);
    }

    private void requireSubscriptionWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.SUBSCRIPTION_WRITE);
    }

    private void requireSubscriptionRenew(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.SUBSCRIPTION_RENEW);
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
