package com.kayledger.api.region.application;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RegionReplicationConsumer {

    private final RegionReplicationService regionReplicationService;

    public RegionReplicationConsumer(RegionReplicationService regionReplicationService) {
        this.regionReplicationService = regionReplicationService;
    }

    @KafkaListener(
            topics = "${kay-ledger.region.replication-topic}",
            groupId = "${kay-ledger.region.local-region-id}-region-replication")
    public void consume(ConsumerRecord<String, String> record) {
        regionReplicationService.apply(record.value());
    }
}
