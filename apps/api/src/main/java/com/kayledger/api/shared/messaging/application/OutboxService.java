package com.kayledger.api.shared.messaging.application;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.events.DomainEventPayload;
import com.kayledger.api.shared.messaging.model.OutboxEvent;
import com.kayledger.api.shared.messaging.store.OutboxStore;

@Service
@EnableConfigurationProperties(AsyncMessagingProperties.class)
public class OutboxService {

    private final OutboxStore outboxStore;
    private final ObjectMapper objectMapper;
    private final AsyncMessagingProperties properties;

    public OutboxService(OutboxStore outboxStore, ObjectMapper objectMapper, AsyncMessagingProperties properties) {
        this.outboxStore = outboxStore;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public OutboxEvent append(UUID workspaceId, String aggregateType, UUID aggregateId, String eventType, String dedupeKey, Map<String, Object> data) {
        UUID eventId = UUID.randomUUID();
        DomainEventPayload payload = new DomainEventPayload(
                eventId,
                workspaceId,
                aggregateType,
                aggregateId,
                eventType,
                dedupeKey,
                Instant.now(),
                data);
        return outboxStore.append(workspaceId, aggregateType, aggregateId, eventType, toJson(payload), dedupeKey);
    }

    public List<OutboxEvent> claimDueBatch() {
        return outboxStore.claimDue(properties.getRelay().getBatchSize());
    }

    public OutboxEvent markPublished(UUID eventId) {
        return outboxStore.markPublished(eventId);
    }

    public OutboxEvent recordFailure(UUID eventId, Exception exception) {
        return outboxStore.markFailure(
                eventId,
                exception.getMessage(),
                properties.getRelay().getMaxAttempts(),
                properties.getRelay().getBackoffSeconds());
    }

    public OutboxEvent replayParked(UUID eventId) {
        return outboxStore.replay(eventId);
    }

    public List<OutboxEvent> listParked(UUID workspaceId) {
        return outboxStore.listParked(workspaceId);
    }

    public List<OutboxEvent> listRecent(UUID workspaceId) {
        return outboxStore.listRecent(workspaceId, 100);
    }

    public String topic() {
        return properties.getKafka().getTopic();
    }

    private String toJson(DomainEventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("Event payload could not be serialized.");
        }
    }
}
