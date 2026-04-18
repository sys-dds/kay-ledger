package com.kayledger.api.shared.messaging.application;

import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.shared.messaging.store.InboxStore;

@Service
public class InboxService {

    private final InboxStore inboxStore;

    public InboxService(InboxStore inboxStore) {
        this.inboxStore = inboxStore;
    }

    @Transactional
    public boolean processOnce(UUID workspaceId, String topic, int partitionId, String messageKey, UUID eventId, String dedupeKey, String consumerName, Supplier<Boolean> handler) {
        boolean inserted = inboxStore.recordSuccess(workspaceId, topic, partitionId, messageKey, eventId, dedupeKey, consumerName);
        if (!inserted) {
            return false;
        }
        return handler.get();
    }
}
