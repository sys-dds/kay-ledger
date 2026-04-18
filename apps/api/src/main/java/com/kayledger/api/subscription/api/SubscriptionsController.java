package com.kayledger.api.subscription.api;

import java.util.List;
import java.util.Map;
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
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.shared.messaging.store.ProjectionStore;
import com.kayledger.api.subscription.application.SubscriptionService;
import com.kayledger.api.subscription.application.SubscriptionService.CreatePlanCommand;
import com.kayledger.api.subscription.application.SubscriptionService.CreateSubscriptionCommand;
import com.kayledger.api.subscription.application.SubscriptionService.RenewalRunCommand;
import com.kayledger.api.subscription.application.SubscriptionService.RenewalRunResult;
import com.kayledger.api.subscription.application.SubscriptionService.SchedulePlanChangeCommand;
import com.kayledger.api.subscription.application.SubscriptionService.SuspensionRunCommand;
import com.kayledger.api.subscription.application.SubscriptionService.SuspensionRunResult;
import com.kayledger.api.subscription.model.EntitlementGrant;
import com.kayledger.api.subscription.model.SubscriptionCycle;
import com.kayledger.api.subscription.model.SubscriptionPlan;
import com.kayledger.api.subscription.model.SubscriptionPlanChange;
import com.kayledger.api.subscription.model.SubscriptionRecord;

@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionsController {

    private final SubscriptionService subscriptionService;
    private final AccessContextResolver accessContextResolver;
    private final IdempotencyService idempotencyService;
    private final ProjectionStore projectionStore;

    public SubscriptionsController(
            SubscriptionService subscriptionService,
            AccessContextResolver accessContextResolver,
            IdempotencyService idempotencyService,
            ProjectionStore projectionStore) {
        this.subscriptionService = subscriptionService;
        this.accessContextResolver = accessContextResolver;
        this.idempotencyService = idempotencyService;
        this.projectionStore = projectionStore;
    }

    @GetMapping("/projections")
    List<Map<String, Object>> projections(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        subscriptionService.listSubscriptions(context);
        return projectionStore.listSubscriptions(context.workspaceId());
    }

    @PostMapping("/plans")
    ResponseEntity<Object> createPlan(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreatePlanCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions/plans",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> subscriptionService.createPlan(context, request));
    }

    @GetMapping("/plans")
    List<SubscriptionPlan> listPlans(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return subscriptionService.listPlans(context);
    }

    @PostMapping
    ResponseEntity<Object> createSubscription(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateSubscriptionCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> subscriptionService.createSubscription(context, request));
    }

    @GetMapping
    List<SubscriptionRecord> listSubscriptions(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return subscriptionService.listSubscriptions(context);
    }

    @PostMapping("/{subscriptionId}/cancel")
    ResponseEntity<Object> cancel(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID subscriptionId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions/{subscriptionId}/cancel",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, subscriptionId),
                () -> subscriptionService.cancel(context, subscriptionId));
    }

    @PostMapping("/{subscriptionId}/plan-changes")
    ResponseEntity<Object> schedulePlanChange(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID subscriptionId,
            @RequestBody SchedulePlanChangeCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions/{subscriptionId}/plan-changes",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, subscriptionId, request),
                () -> subscriptionService.schedulePlanChange(context, subscriptionId, request));
    }

    @PostMapping("/renewals/run")
    ResponseEntity<Object> runDueRenewals(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) RenewalRunCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions/renewals/run",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> subscriptionService.runDueRenewals(context, request));
    }

    @PostMapping("/suspensions/run")
    ResponseEntity<Object> runGraceSuspension(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody(required = false) SuspensionRunCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/subscriptions/suspensions/run",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, request),
                () -> subscriptionService.transitionGraceToSuspension(context, request));
    }

    @GetMapping("/{subscriptionId}/cycles")
    List<SubscriptionCycle> listCycles(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID subscriptionId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return subscriptionService.listCycles(context, subscriptionId);
    }

    @GetMapping("/{subscriptionId}/entitlements")
    List<EntitlementGrant> listEntitlements(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable UUID subscriptionId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return subscriptionService.listEntitlements(context, subscriptionId);
    }
}
