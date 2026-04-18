package com.kayledger.api.shared.messaging.application;

import java.util.List;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.kayledger.api.shared.messaging.model.OutboxEvent;

@Service
public class OutboxRelayService {

    private final OutboxService outboxService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayService(OutboxService outboxService, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxService = outboxService;
        this.kafkaTemplate = kafkaTemplate;
    }

    public RelayResult relayDue() {
        List<OutboxEvent> events = outboxService.claimDueBatch();
        int published = 0;
        int failed = 0;
        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(outboxService.topic(), event.dedupeKey(), event.payloadJson()).get();
                outboxService.markPublished(event.id());
                published++;
            } catch (Exception exception) {
                outboxService.recordFailure(event.id(), exception);
                failed++;
            }
        }
        return new RelayResult(events.size(), published, failed);
    }

    public record RelayResult(int claimed, int published, int failed) {
    }
}
