package com.kayledger.api.shared.messaging.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.shared.idempotency.IdempotencyService;
import com.kayledger.api.shared.messaging.application.InboxService;
import com.kayledger.api.shared.messaging.application.OutboxRelayService;
import com.kayledger.api.shared.messaging.application.OutboxService;
import com.kayledger.api.shared.messaging.application.ProjectionConsumer;
import com.kayledger.api.shared.messaging.model.OutboxEvent;
import com.kayledger.api.shared.api.BadRequestException;

@RestController
@RequestMapping("/api/messaging")
public class MessagingOperatorController {

    private final AccessContextResolver accessContextResolver;
    private final AccessPolicy accessPolicy;
    private final OutboxService outboxService;
    private final OutboxRelayService outboxRelayService;
    private final InboxService inboxService;
    private final ProjectionConsumer projectionConsumer;
    private final IdempotencyService idempotencyService;

    public MessagingOperatorController(
            AccessContextResolver accessContextResolver,
            AccessPolicy accessPolicy,
            OutboxService outboxService,
            OutboxRelayService outboxRelayService,
            InboxService inboxService,
            ProjectionConsumer projectionConsumer,
            IdempotencyService idempotencyService) {
        this.accessContextResolver = accessContextResolver;
        this.accessPolicy = accessPolicy;
        this.outboxService = outboxService;
        this.outboxRelayService = outboxRelayService;
        this.inboxService = inboxService;
        this.projectionConsumer = projectionConsumer;
        this.idempotencyService = idempotencyService;
    }

    @GetMapping("/outbox")
    List<OutboxEvent> outboxStatus(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return outboxService.listRecent(context.workspaceId());
    }

    @PostMapping("/outbox/relay")
    ResponseEntity<Object> relay(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/messaging/outbox/relay",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, Map.of("relay", true)),
                () -> outboxRelayService.relayDue(context.workspaceId()));
    }

    @GetMapping("/outbox/parked")
    List<OutboxEvent> parked(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return outboxService.listParked(context.workspaceId());
    }

    @PostMapping("/outbox/parked/{eventId}/replay")
    ResponseEntity<Object> replay(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable UUID eventId) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/messaging/outbox/parked/{eventId}/replay",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, eventId),
                () -> outboxService.replayParked(context.workspaceId(), eventId));
    }

    @GetMapping("/inbox/parked")
    List<Map<String, Object>> parkedInbox(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return inboxService.listParked(context.workspaceId());
    }

    @GetMapping("/inbox/parked/{consumerName}/{dedupeKey}")
    Map<String, Object> parkedInboxDetail(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @PathVariable String consumerName,
            @PathVariable String dedupeKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        var parked = inboxService.parkedMessage(context.workspaceId(), consumerName, dedupeKey);
        return Map.of(
                "workspaceId", parked.workspaceId(),
                "consumerName", consumerName,
                "dedupeKey", parked.dedupeKey(),
                "topic", parked.topic(),
                "eventId", parked.eventId(),
                "hasPayload", parked.payloadJson() != null && !parked.payloadJson().isBlank());
    }

    @PostMapping("/inbox/parked/{consumerName}/{dedupeKey}/replay")
    ResponseEntity<Object> replayInbox(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @PathVariable String consumerName,
            @PathVariable String dedupeKey) {
        AccessContext context = operatorContext(workspaceSlug, actorKey);
        return idempotencyService.run(
                idempotencyKey,
                "WORKSPACE",
                context.workspaceId(),
                context.actorId(),
                "POST /api/messaging/inbox/parked/{consumerName}/{dedupeKey}/replay",
                IdempotencyService.fingerprint(workspaceSlug, actorKey, consumerName, dedupeKey),
                () -> {
                    if (!"projection-consumer".equals(consumerName)) {
                        throw new BadRequestException("Replay is only supported for projection-consumer messages.");
                    }
                    return Map.of("replayed", projectionConsumer.replayParked(context.workspaceId(), dedupeKey));
                });
    }

    private AccessContext operatorContext(String workspaceSlug, String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        return context;
    }
}
