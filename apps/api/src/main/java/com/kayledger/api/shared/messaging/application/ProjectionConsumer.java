package com.kayledger.api.shared.messaging.application;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.shared.events.DomainEventPayload;
import com.kayledger.api.shared.messaging.store.ProjectionStore;

@Component
public class ProjectionConsumer {

    private static final String CONSUMER_NAME = "projection-consumer";

    private final InboxService inboxService;
    private final ProjectionStore projectionStore;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;

    public ProjectionConsumer(InboxService inboxService, ProjectionStore projectionStore, ObjectMapper objectMapper, InvestigationIndexingService investigationIndexingService) {
        this.inboxService = inboxService;
        this.projectionStore = projectionStore;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
    }

    @KafkaListener(
            topics = "${kay-ledger.async.kafka.topic}",
            groupId = "${kay-ledger.async.kafka.consumer-group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        DomainEventPayload event = payload(record.value());
        inboxService.processOnce(
                event.workspaceId(),
                record.topic(),
                record.partition(),
                record.key(),
                event.eventId(),
                event.dedupeKey(),
                CONSUMER_NAME,
                record.value(),
                () -> {
                    apply(event, event.workspaceId());
                    reindex(event.workspaceId(), event);
                    return true;
                });
    }

    public boolean replayParked(UUID workspaceId, String dedupeKey) {
        var parked = inboxService.parkedMessage(workspaceId, CONSUMER_NAME, dedupeKey);
        if (parked.payloadJson() == null || parked.payloadJson().isBlank()) {
            throw new IllegalStateException("Parked inbox message has no payload to replay.");
        }
        inboxService.replayParked(workspaceId, CONSUMER_NAME, dedupeKey);
        DomainEventPayload event = payload(parked.payloadJson());
        return inboxService.processOnce(
                parked.workspaceId(),
                parked.topic(),
                parked.partitionId(),
                parked.messageKey(),
                parked.eventId(),
                parked.dedupeKey(),
                CONSUMER_NAME,
                parked.payloadJson(),
                () -> {
                    apply(event, parked.workspaceId());
                    reindex(parked.workspaceId(), event);
                    return true;
                });
    }

    public void apply(DomainEventPayload event) {
        apply(event, event.workspaceId());
    }

    private void apply(DomainEventPayload event, UUID workspaceId) {
        if (event.eventType().startsWith("payment.")) {
            projectionStore.upsertPayment(workspaceId, event.data());
        }
        if (event.eventType().startsWith("subscription.")) {
            UUID subscriptionId = subscriptionId(event);
            if (subscriptionId != null) {
                projectionStore.upsertSubscription(workspaceId, subscriptionId, event.data());
            }
        }
    }

    private void reindex(UUID workspaceId, DomainEventPayload event) {
        try {
            investigationIndexingService.indexReference(workspaceId, investigationReferenceType(event), event.aggregateId());
        } catch (RuntimeException ignored) {
            // Search indexing is replay-safe and re-driveable; projection processing remains source-of-truth first.
        }
    }

    private static String investigationReferenceType(DomainEventPayload event) {
        return switch (event.aggregateType()) {
            case "PAYMENT_INTENT" -> "PAYMENT_INTENT";
            case "SUBSCRIPTION" -> "SUBSCRIPTION";
            case "SUBSCRIPTION_CYCLE" -> "SUBSCRIPTION_CYCLE";
            default -> event.aggregateType();
        };
    }

    private DomainEventPayload payload(String value) {
        try {
            Map<String, Object> raw = objectMapper.readValue(value, new TypeReference<>() {
            });
            return new DomainEventPayload(
                    UUID.fromString(raw.get("eventId").toString()),
                    raw.get("workspaceId") == null ? null : UUID.fromString(raw.get("workspaceId").toString()),
                    raw.get("aggregateType").toString(),
                    UUID.fromString(raw.get("aggregateId").toString()),
                    raw.get("eventType").toString(),
                    raw.get("dedupeKey").toString(),
                    null,
                    objectMapper.convertValue(raw.get("data"), new TypeReference<>() {
                    }));
        } catch (Exception exception) {
            throw new IllegalArgumentException("Event payload could not be consumed.", exception);
        }
    }

    private static UUID subscriptionId(DomainEventPayload event) {
        Object value = event.data().get("subscriptionId");
        if (value == null && "SUBSCRIPTION".equals(event.aggregateType())) {
            return event.aggregateId();
        }
        return value == null ? null : UUID.fromString(value.toString());
    }
}
