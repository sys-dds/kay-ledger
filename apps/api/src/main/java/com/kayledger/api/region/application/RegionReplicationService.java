package com.kayledger.api.region.application;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.kayledger.api.region.recovery.store.RegionalRecoveryStore;
import com.kayledger.api.region.store.RegionStore;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Service
public class RegionReplicationService {

    public static final String INVESTIGATION_READ_SNAPSHOT = "INVESTIGATION_READ_SNAPSHOT";
    public static final String PROVIDER_SUMMARY_SNAPSHOT = "PROVIDER_SUMMARY_SNAPSHOT";
    public static final String WORKSPACE_OWNERSHIP_TRANSFER = "WORKSPACE_OWNERSHIP_TRANSFER";
    public static final String RECOVERY_ACTION_CONFIRMATION = "RECOVERY_ACTION_CONFIRMATION";
    private static final Logger log = LoggerFactory.getLogger(RegionReplicationService.class);

    private final RegionStore regionStore;
    private final RegionProperties regionProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final RegionFaultService regionFaultService;
    private final RegionalRecoveryStore regionalRecoveryStore;

    public RegionReplicationService(RegionStore regionStore, RegionProperties regionProperties, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, RegionFaultService regionFaultService) {
        this(regionStore, regionProperties, kafkaTemplate, objectMapper, regionFaultService, null);
    }

    @Autowired
    public RegionReplicationService(RegionStore regionStore, RegionProperties regionProperties, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, RegionFaultService regionFaultService, RegionalRecoveryStore regionalRecoveryStore) {
        this.regionStore = regionStore;
        this.regionProperties = regionProperties;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.regionFaultService = regionFaultService;
        this.regionalRecoveryStore = regionalRecoveryStore;
    }

    public void publishInvestigationDocument(InvestigationDocument document) {
        publishInvestigationDocument(document, null);
    }

    public void publishInvestigationDocument(InvestigationDocument document, UUID recoveryActionId) {
        if (!regionProperties.isReplicationProducerEnabled()) {
            return;
        }
        for (String peerRegion : regionProperties.getPeerRegionIds()) {
            publish(INVESTIGATION_READ_SNAPSHOT, document.workspaceId(), peerRegion, recoveryPayload("document", document, recoveryActionId, "REPLAY_INVESTIGATION_SNAPSHOT", document.referenceType(), document.referenceId().toString()));
        }
    }

    public void publishProviderSummary(ProviderFinancialSummary summary) {
        publishProviderSummary(summary, null);
    }

    public void publishProviderSummary(ProviderFinancialSummary summary, UUID recoveryActionId) {
        if (!regionProperties.isReplicationProducerEnabled()) {
            return;
        }
        for (String peerRegion : regionProperties.getPeerRegionIds()) {
            publish(PROVIDER_SUMMARY_SNAPSHOT, summary.workspaceId(), peerRegion, recoveryPayload("summary", summary, recoveryActionId, "REPLAY_PROVIDER_SUMMARY_SNAPSHOT", PROVIDER_SUMMARY_SNAPSHOT, summary.providerProfileId().toString()));
        }
    }

    public void publishOwnershipTransfer(UUID workspaceId, String fromRegion, String toRegion, long priorEpoch, long newEpoch, String triggerMode, UUID actorId) {
        publishOwnershipTransfer(workspaceId, fromRegion, toRegion, priorEpoch, newEpoch, triggerMode, actorId, null);
    }

    public void publishOwnershipTransfer(UUID workspaceId, String fromRegion, String toRegion, long priorEpoch, long newEpoch, String triggerMode, UUID actorId, UUID recoveryActionId) {
        if (!regionProperties.isReplicationProducerEnabled()) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("fromRegion", fromRegion);
        payload.put("toRegion", toRegion);
        payload.put("priorEpoch", priorEpoch);
        payload.put("newEpoch", newEpoch);
        payload.put("triggerMode", triggerMode);
        payload.put("requestedByActorId", actorId == null ? null : actorId.toString());
        putRecovery(payload, recoveryActionId, "REPLAY_OWNERSHIP_TRANSFER", "WORKSPACE", workspaceId.toString());
        for (String peerRegion : regionProperties.getPeerRegionIds()) {
            publish(WORKSPACE_OWNERSHIP_TRANSFER, workspaceId, peerRegion, payload);
        }
    }

    @Transactional
    public void apply(String payloadJson) {
        RegionReplicationEvent event = readEvent(payloadJson);
        if (!regionProperties.isReplicationConsumerEnabled() || !regionProperties.getLocalRegionId().equals(event.targetRegion())) {
            return;
        }
        if (regionFaultService.active(event.workspaceId(), RegionFaultService.REGIONAL_REPLICATION_APPLY_BLOCK)) {
            return;
        }
        regionFaultService.simulateDelayIfActive(event.workspaceId(), RegionFaultService.REGIONAL_REPLICATION_APPLY_DELAY);
        if (INVESTIGATION_READ_SNAPSHOT.equals(event.streamName())) {
            InvestigationDocument document = objectMapper.convertValue(event.payload().get("document"), InvestigationDocument.class);
            String documentJson = writeJson(document);
            boolean applied = regionStore.upsertInvestigationSnapshot(event, document, documentJson, lagMillis(event.occurredAt()));
            if (applied) {
                publishRecoveryConfirmationIfPresent(event);
            }
            return;
        }
        if (PROVIDER_SUMMARY_SNAPSHOT.equals(event.streamName())) {
            ProviderFinancialSummary summary = objectMapper.convertValue(event.payload().get("summary"), ProviderFinancialSummary.class);
            boolean applied = regionStore.upsertProviderSummarySnapshot(event, summary, lagMillis(event.occurredAt()));
            if (applied) {
                publishRecoveryConfirmationIfPresent(event);
            }
            return;
        }
        if (WORKSPACE_OWNERSHIP_TRANSFER.equals(event.streamName())) {
            String fromRegion = text(event.payload().get("fromRegion"));
            String toRegion = text(event.payload().get("toRegion"));
            long priorEpoch = number(event.payload().get("priorEpoch"));
            long newEpoch = number(event.payload().get("newEpoch"));
            String triggerMode = text(event.payload().get("triggerMode"));
            UUID requestedByActorId = optionalUuid(event.payload().get("requestedByActorId"));
            regionStore.upsertReplicatedOwnership(event.workspaceId(), toRegion, newEpoch);
            regionStore.recordReplicatedFailover(event.workspaceId(), fromRegion, toRegion, priorEpoch, newEpoch, triggerMode, requestedByActorId);
            regionStore.upsertReplicationCheckpoint(event, lagMillis(event.occurredAt()));
            publishRecoveryConfirmationIfPresent(event);
            return;
        }
        if (RECOVERY_ACTION_CONFIRMATION.equals(event.streamName())) {
            applyRecoveryConfirmation(event);
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
        if (regionStore.findOwnership(workspaceId)
                .map(ownership -> regionProperties.getLocalRegionId().equals(ownership.homeRegion()))
                .orElse(false)) {
            return new RegionReadFreshness("LOCAL_SOURCE_OF_TRUTH", regionProperties.getLocalRegionId(), regionProperties.getLocalRegionId(), 0, null);
        }
        return regionStore.latestCheckpointForTarget(regionProperties.getLocalRegionId(), streamName, workspaceId)
                .map(checkpoint -> new RegionReadFreshness(
                        "REPLICATED_SNAPSHOT",
                        checkpoint.sourceRegion(),
                        checkpoint.targetRegion(),
                        checkpoint.lagMillis(),
                        checkpoint.lastAppliedAt()))
                .orElse(new RegionReadFreshness("NO_REPLICATED_CHECKPOINT", null, regionProperties.getLocalRegionId(), 0, null));
    }

    public List<RegionReplicationCheckpoint> checkpoints() {
        return regionStore.replicationCheckpoints(regionProperties.getLocalRegionId());
    }

    private void publish(String streamName, UUID workspaceId, String targetRegion, Map<String, Object> payload) {
        if (kafkaTemplate == null) {
            return;
        }
        if (regionFaultService.active(workspaceId, RegionFaultService.REGIONAL_REPLICATION_PUBLISH_BLOCK)) {
            return;
        }
        regionFaultService.simulateDelayIfActive(workspaceId, RegionFaultService.REGIONAL_REPLICATION_PUBLISH_DELAY);
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

    private void publishRecoveryConfirmationIfPresent(RegionReplicationEvent appliedEvent) {
        UUID recoveryActionId = optionalUuid(appliedEvent.payload().get("recoveryActionId"));
        if (recoveryActionId == null || !regionProperties.isReplicationProducerEnabled() || kafkaTemplate == null) {
            if (regionalRecoveryStore != null && recoveryActionId != null) {
                recordPeerConfirmation(appliedEvent, recoveryActionId, UUID.randomUUID());
            }
            return;
        }
        UUID confirmationEventId = UUID.randomUUID();
        recordPeerConfirmation(appliedEvent, recoveryActionId, confirmationEventId);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("recoveryActionId", recoveryActionId.toString());
        payload.put("actionType", text(appliedEvent.payload().get("recoveryActionType")));
        payload.put("referenceType", text(appliedEvent.payload().get("recoveryReferenceType")));
        payload.put("referenceId", text(appliedEvent.payload().get("recoveryReferenceId")));
        payload.put("appliedRegion", regionProperties.getLocalRegionId());
        payload.put("applyEventId", appliedEvent.eventId().toString());
        payload.put("appliedAt", Instant.now().toString());
        RegionReplicationEvent confirmation = new RegionReplicationEvent(
                confirmationEventId,
                System.currentTimeMillis(),
                regionProperties.getLocalRegionId(),
                appliedEvent.sourceRegion(),
                RECOVERY_ACTION_CONFIRMATION,
                appliedEvent.workspaceId(),
                Instant.now(),
                payload);
        try {
            kafkaTemplate.send(regionProperties.getReplicationTopic(), appliedEvent.workspaceId().toString(), writeJson(confirmation));
        } catch (RuntimeException exception) {
            log.warn("Regional recovery confirmation publish failed for action {} workspace {}", recoveryActionId, appliedEvent.workspaceId(), exception);
        }
    }

    private void applyRecoveryConfirmation(RegionReplicationEvent event) {
        if (regionalRecoveryStore == null) {
            return;
        }
        UUID recoveryActionId = optionalUuid(event.payload().get("recoveryActionId"));
        if (recoveryActionId == null) {
            return;
        }
        UUID applyEventId = optionalUuid(event.payload().get("applyEventId"));
        Instant appliedAt = Instant.parse(text(event.payload().get("appliedAt")));
        regionalRecoveryStore.recordPeerConfirmation(
                event.workspaceId(),
                recoveryActionId,
                text(event.payload().get("actionType")),
                text(event.payload().get("referenceType")),
                text(event.payload().get("referenceId")),
                text(event.payload().get("appliedRegion")),
                event.sourceRegion(),
                applyEventId,
                event.eventId(),
                appliedAt);
        regionalRecoveryStore.completeActionFromPeerConfirmation(
                event.workspaceId(),
                recoveryActionId,
                text(event.payload().get("appliedRegion")),
                event.eventId(),
                appliedAt,
                writeJson(Map.of("status", "peer_applied", "appliedRegion", text(event.payload().get("appliedRegion")))));
        regionStore.upsertReplicationCheckpoint(event, lagMillis(event.occurredAt()));
    }

    private void recordPeerConfirmation(RegionReplicationEvent appliedEvent, UUID recoveryActionId, UUID confirmationEventId) {
        if (regionalRecoveryStore == null) {
            return;
        }
        regionalRecoveryStore.recordPeerConfirmation(
                appliedEvent.workspaceId(),
                recoveryActionId,
                text(appliedEvent.payload().get("recoveryActionType")),
                text(appliedEvent.payload().get("recoveryReferenceType")),
                text(appliedEvent.payload().get("recoveryReferenceId")),
                regionProperties.getLocalRegionId(),
                appliedEvent.sourceRegion(),
                appliedEvent.eventId(),
                confirmationEventId,
                Instant.now());
    }

    private static Map<String, Object> recoveryPayload(String key, Object value, UUID recoveryActionId, String actionType, String referenceType, String referenceId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put(key, value);
        putRecovery(payload, recoveryActionId, actionType, referenceType, referenceId);
        return payload;
    }

    private static void putRecovery(Map<String, Object> payload, UUID recoveryActionId, String actionType, String referenceType, String referenceId) {
        if (recoveryActionId != null) {
            payload.put("recoveryActionId", recoveryActionId.toString());
            payload.put("recoveryActionType", actionType);
            payload.put("recoveryReferenceType", referenceType);
            payload.put("recoveryReferenceId", referenceId);
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

    private static String text(Object value) {
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("Regional replication payload field is required.");
        }
        return value.toString();
    }

    private static long number(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(text(value));
    }

    private static UUID optionalUuid(Object value) {
        if (value == null || value.toString().isBlank()) {
            return null;
        }
        return UUID.fromString(value.toString());
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
