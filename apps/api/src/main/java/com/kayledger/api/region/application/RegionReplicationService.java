package com.kayledger.api.region.application;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.investigation.model.InvestigationSearchHit;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.model.RegionInvestigationSnapshot;
import com.kayledger.api.region.model.RegionProviderSummarySnapshot;
import com.kayledger.api.region.model.RegionReadFreshness;
import com.kayledger.api.region.model.RegionReplicationCheckpoint;
import com.kayledger.api.region.store.RegionStore;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Service
public class RegionReplicationService {

    public static final String INVESTIGATION_READ_SNAPSHOT = "INVESTIGATION_READ_SNAPSHOT";
    public static final String PROVIDER_SUMMARY_SNAPSHOT = "PROVIDER_SUMMARY_SNAPSHOT";
    private static final Logger log = LoggerFactory.getLogger(RegionReplicationService.class);

    private final RegionStore regionStore;
    private final RegionProperties regionProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public RegionReplicationService(RegionStore regionStore, RegionProperties regionProperties, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.regionStore = regionStore;
        this.regionProperties = regionProperties;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishInvestigationDocument(InvestigationDocument document) {
        if (!regionProperties.isReplicationProducerEnabled()) {
            return;
        }
        for (String peerRegion : regionProperties.getPeerRegionIds()) {
            publish(INVESTIGATION_READ_SNAPSHOT, document.workspaceId(), peerRegion, Map.of("document", document));
        }
    }

    public void publishProviderSummary(ProviderFinancialSummary summary) {
        if (!regionProperties.isReplicationProducerEnabled()) {
            return;
        }
        for (String peerRegion : regionProperties.getPeerRegionIds()) {
            publish(PROVIDER_SUMMARY_SNAPSHOT, summary.workspaceId(), peerRegion, Map.of("summary", summary));
        }
    }

    @Transactional
    public void apply(String payloadJson) {
        RegionReplicationEvent event = readEvent(payloadJson);
        if (!regionProperties.isReplicationConsumerEnabled() || !regionProperties.getLocalRegionId().equals(event.targetRegion())) {
            return;
        }
        if (INVESTIGATION_READ_SNAPSHOT.equals(event.streamName())) {
            InvestigationDocument document = objectMapper.convertValue(event.payload().get("document"), InvestigationDocument.class);
            String documentJson = writeJson(document);
            regionStore.upsertInvestigationSnapshot(event, document, documentJson, lagMillis(event.occurredAt()));
            return;
        }
        if (PROVIDER_SUMMARY_SNAPSHOT.equals(event.streamName())) {
            ProviderFinancialSummary summary = objectMapper.convertValue(event.payload().get("summary"), ProviderFinancialSummary.class);
            regionStore.upsertProviderSummarySnapshot(event, summary, lagMillis(event.occurredAt()));
        }
    }

    public List<InvestigationSearchHit> searchInvestigationSnapshots(
            UUID workspaceId,
            String paymentId,
            String refundId,
            String payoutId,
            String disputeId,
            String providerEventId,
            String externalReference,
            String businessReferenceId,
            String subscriptionId,
            String providerProfileId,
            String referenceId) {
        return regionStore.searchInvestigationSnapshots(
                        regionProperties.getLocalRegionId(),
                        workspaceId,
                        paymentId,
                        refundId,
                        payoutId,
                        disputeId,
                        providerEventId,
                        externalReference,
                        businessReferenceId,
                        subscriptionId,
                        providerProfileId,
                        referenceId)
                .stream()
                .map(this::toHit)
                .toList();
    }

    public List<ProviderFinancialSummary> providerSummarySnapshots(UUID workspaceId) {
        return regionStore.providerSummarySnapshots(regionProperties.getLocalRegionId(), workspaceId)
                .stream()
                .map(RegionProviderSummarySnapshot::summary)
                .toList();
    }

    public RegionReadFreshness freshness(String streamName, UUID workspaceId) {
        return regionStore.latestCheckpointForTarget(regionProperties.getLocalRegionId(), streamName)
                .map(checkpoint -> new RegionReadFreshness(
                        "REPLICATED_SNAPSHOT",
                        checkpoint.sourceRegion(),
                        checkpoint.targetRegion(),
                        checkpoint.lagMillis(),
                        checkpoint.lastAppliedAt()))
                .orElse(new RegionReadFreshness("LOCAL_SOURCE_OF_TRUTH", regionProperties.getLocalRegionId(), regionProperties.getLocalRegionId(), 0, null));
    }

    public List<RegionReplicationCheckpoint> checkpoints() {
        return regionStore.replicationCheckpoints(regionProperties.getLocalRegionId());
    }

    private void publish(String streamName, UUID workspaceId, String targetRegion, Map<String, Object> payload) {
        RegionReplicationEvent event = new RegionReplicationEvent(
                UUID.randomUUID(),
                System.currentTimeMillis(),
                regionProperties.getLocalRegionId(),
                targetRegion,
                streamName,
                workspaceId,
                Instant.now(),
                payload);
        try {
            kafkaTemplate.send(regionProperties.getReplicationTopic(), workspaceId.toString(), writeJson(event));
        } catch (RuntimeException exception) {
            log.warn("Regional replication publish failed for stream {} workspace {} target {}", streamName, workspaceId, targetRegion, exception);
        }
    }

    private InvestigationSearchHit toHit(RegionInvestigationSnapshot snapshot) {
        Map<String, Object> source = readMap(snapshot.payloadJson());
        source.put("readSource", "REPLICATED_SNAPSHOT");
        source.put("sourceRegion", snapshot.sourceRegion());
        source.put("replicatedAt", snapshot.replicatedAt().toString());
        return new InvestigationSearchHit(
                snapshot.documentId(),
                snapshot.documentType(),
                snapshot.referenceType(),
                snapshot.referenceId(),
                snapshot.status(),
                source);
    }

    private RegionReplicationEvent readEvent(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, RegionReplicationEvent.class);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Regional replication event could not be parsed.", exception);
        }
    }

    private Map<String, Object> readMap(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Regional snapshot payload could not be parsed.", exception);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Regional replication payload could not be serialized.", exception);
        }
    }

    private static long lagMillis(Instant occurredAt) {
        return Math.max(Duration.between(occurredAt, Instant.now()).toMillis(), 0);
    }

    public record RegionReplicationEvent(
            UUID eventId,
            long sequence,
            String sourceRegion,
            String targetRegion,
            String streamName,
            UUID workspaceId,
            Instant occurredAt,
            Map<String, Object> payload) {
    }
}
