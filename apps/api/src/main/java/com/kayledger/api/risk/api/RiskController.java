package com.kayledger.api.risk.api;

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
import com.kayledger.api.risk.application.RiskService;
import com.kayledger.api.risk.application.RiskService.DecisionCommand;
import com.kayledger.api.risk.model.RiskDecision;
import com.kayledger.api.risk.model.RiskFlag;
import com.kayledger.api.risk.model.RiskReview;
import com.kayledger.api.shared.idempotency.IdempotencyService;

@RestController
@RequestMapping("/api/risk")
public class RiskController {

    private final AccessContextResolver accessContextResolver;
    private final RiskService riskService;
    private final IdempotencyService idempotencyService;

    public RiskController(AccessContextResolver accessContextResolver, RiskService riskService, IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.riskService = riskService;
        this.idempotencyService = idempotencyService;
    }

    @GetMapping("/flags")
    List<RiskFlag> flags(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return riskService.listFlags(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/reviews")
    List<RiskReview> reviews(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return riskService.listReviewQueue(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/reviews/{reviewId}/in-review")
    ResponseEntity<Object> markInReview(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID reviewId) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(idempotencyKey, "WORKSPACE", context.workspaceId(), context.actorId(), "POST /api/risk/reviews/{reviewId}/in-review", IdempotencyService.fingerprint(workspaceSlug, actorKey, reviewId), () -> riskService.markInReview(context, reviewId));
    }

    @PostMapping("/reviews/{reviewId}/decisions")
    ResponseEntity<Object> decide(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID reviewId,
            @RequestBody DecisionCommand request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return idempotencyService.run(idempotencyKey, "WORKSPACE", context.workspaceId(), context.actorId(), "POST /api/risk/reviews/{reviewId}/decisions", IdempotencyService.fingerprint(workspaceSlug, actorKey, reviewId, request), () -> riskService.decide(context, reviewId, request));
    }

    @GetMapping("/decisions")
    List<RiskDecision> decisions(@RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug, @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return riskService.listDecisions(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }
}
