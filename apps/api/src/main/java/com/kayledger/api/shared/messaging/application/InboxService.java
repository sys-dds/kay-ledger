package com.kayledger.api.shared.messaging.application;

import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import com.kayledger.api.shared.messaging.store.InboxStore;

@Service
@EnableConfigurationProperties(AsyncMessagingProperties.class)
public class InboxService {

    private final InboxStore inboxStore;
    private final AsyncMessagingProperties properties;

    public InboxService(InboxStore inboxStore, AsyncMessagingProperties properties) {
        this.inboxStore = inboxStore;
        this.properties = properties;
    }

    public boolean processOnce(UUID workspaceId, String topic, int partitionId, String messageKey, UUID eventId, String dedupeKey, String consumerName, Supplier<Boolean> handler) {
        boolean claimed = inboxStore.beginProcessing(workspaceId, topic, partitionId, messageKey, eventId, dedupeKey, consumerName);
        if (!claimed) {
            return false;
        }
        try {
            boolean handled = handler.get();
            if (handled) {
                inboxStore.recordSuccess(dedupeKey, consumerName);
            }
            return handled;
        } catch (RuntimeException exception) {
            boolean parked = inboxStore.recordFailure(
                    dedupeKey,
                    consumerName,
                    exception.getMessage(),
                    properties.getRelay().getMaxAttempts(),
                    properties.getRelay().getBackoffSeconds());
            if (!parked) {
                throw exception;
            }
            return false;
        }
    }

    public int replayParked(UUID workspaceId, String consumerName, String dedupeKey) {
        return inboxStore.replayParked(workspaceId, consumerName, dedupeKey);
    }

    public List<Map<String, Object>> listParked(UUID workspaceId) {
        return inboxStore.listParked(workspaceId);
    }
}
