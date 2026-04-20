package com.kayledger.api.region.recovery.application;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.provider.application.ProviderCallbackService;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.model.WorkspaceRegionFailoverEvent;
import com.kayledger.api.region.recovery.model.RegionalDriftRecord;
import com.kayledger.api.region.recovery.model.RegionalRecoveryAction;
import com.kayledger.api.region.recovery.store.RegionalRecoveryStore;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class RegionalRecoveryService {

    public static final String DELAYED_PROVIDER_CALLBACK_BACKLOG = "DELAYED_PROVIDER_CALLBACK_BACKLOG";
    public static final String SNAPSHOT_CHECKPOINT_WITHOUT_ROW = "SNAPSHOT_CHECKPOINT_WITHOUT_ROW";
    public static final String REGIONAL_READ_SNAPSHOT_MISSING = "REGIONAL_READ_SNAPSHOT_MISSING";

    public static final String REPLAY_OWNERSHIP_TRANSFER = "REPLAY_OWNERSHIP_TRANSFER";
    public static final String REDRIVE_DELAYED_PROVIDER_CALLBACK = "REDRIVE_DELAYED_PROVIDER_CALLBACK";
    public static final String REPLAY_INVESTIGATION_SNAPSHOT = "REPLAY_INVESTIGATION_SNAPSHOT";
    public static final String REPLAY_PROVIDER_SUMMARY_SNAPSHOT = "REPLAY_PROVIDER_SUMMARY_SNAPSHOT";

    private final RegionalRecoveryStore recoveryStore;
    private final RegionService regionService;
    private final RegionReplicationService regionReplicationService;
    private final ProviderCallbackService providerCallbackService;
    private final InvestigationIndexingService investigationIndexingService;
    private final ReportingService reportingService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public RegionalRecoveryService(
            RegionalRecoveryStore recoveryStore,
            RegionService regionService,
            RegionReplicationService regionReplicationService,
            ProviderCallbackService providerCallbackService,
            InvestigationIndexingService investigationIndexingService,
            ReportingService reportingService,
            AccessPolicy accessPolicy,
            ObjectMapper objectMapper) {
        this.recoveryStore = recoveryStore;
        this.regionService = regionService;
        this.regionReplicationService = regionReplicationService;
        this.providerCallbackService = providerCallbackService;
        this.investigationIndexingService = investigationIndexingService;
        this.reportingService = reportingService;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public List<RegionalDriftRecord> scan(AccessContext context) {
        requireWrite(context);
        UUID workspaceId = context.workspaceId();
        for (var row : recoveryStore.delayedCallbacks(workspaceId)) {
            recoveryStore.upsertOpenDrift(
                    workspaceId,
                    DELAYED_PROVIDER_CALLBACK_BACKLOG,
                    regionService.localRegionId(),
                    regionService.localRegionId(),
                    "PROVIDER_CALLBACK",
                    row.callbackId().toString(),
                    json(Map.of("callbackId", row.callbackId().toString())));
        }
        for (var drift : recoveryStore.snapshotCheckpointDrifts(workspaceId)) {
            recoveryStore.upsertOpenDrift(
                    workspaceId,
                    SNAPSHOT_CHECKPOINT_WITHOUT_ROW,
                    drift.sourceRegion(),
                    drift.targetRegion(),
                    drift.streamName(),
                    workspaceId + ":" + drift.lastAppliedSequence(),
                    json(Map.of("streamName", drift.streamName(), "lastAppliedSequence", drift.lastAppliedSequence())));
        }
        if (!regionService.isLocalOwner(workspaceId)) {
            if (!recoveryStore.hasProviderSummarySnapshotRows(workspaceId, regionService.localRegionId())) {
                recoveryStore.upsertOpenDrift(
                        workspaceId,
                        REGIONAL_READ_SNAPSHOT_MISSING,
                        null,
                        regionService.localRegionId(),
                        RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT,
                        workspaceId.toString(),
                        json(Map.of("surface", "provider summary")));
            }
            if (!recoveryStore.hasInvestigationSnapshotRows(workspaceId, regionService.localRegionId())) {
                recoveryStore.upsertOpenDrift(
                        workspaceId,
                        REGIONAL_READ_SNAPSHOT_MISSING,
                        null,
                        regionService.localRegionId(),
                        RegionReplicationService.INVESTIGATION_READ_SNAPSHOT,
                        workspaceId.toString(),
                        json(Map.of("surface", "investigation read")));
            }
        }
        return recoveryStore.listUnresolvedDrift(workspaceId);
    }

    public List<RegionalDriftRecord> listDrift(AccessContext context) {
        requireRead(context);
        return recoveryStore.listDrift(context.workspaceId());
    }

    public List<RegionalDriftRecord> listUnresolvedDrift(AccessContext context) {
        requireRead(context);
        return recoveryStore.listUnresolvedDrift(context.workspaceId());
    }

    public List<RegionalRecoveryAction> listActions(AccessContext context) {
        requireRead(context);
        return recoveryStore.listActions(context.workspaceId());
    }

    @Transactional
    public RegionalRecoveryAction requestRecovery(AccessContext context, RecoveryCommand command) {
        requireWrite(context);
        if (command == null) {
            throw new BadRequestException("recovery command is required.");
        }
        RegionalDriftRecord drift = command.driftRecordId() == null
                ? null
                : recoveryStore.findDrift(context.workspaceId(), command.driftRecordId())
                        .orElseThrow(() -> new NotFoundException("Regional drift record was not found."));
        String actionType = requireAction(command.actionType());
        String referenceType = command.referenceType() == null || command.referenceType().isBlank()
                ? drift == null ? null : drift.referenceType()
                : command.referenceType().trim();
        String referenceId = command.referenceId() == null || command.referenceId().isBlank()
                ? drift == null ? null : drift.referenceId()
                : command.referenceId().trim();
        if (referenceType == null || referenceId == null) {
            throw new BadRequestException("referenceType and referenceId are required.");
        }
        RegionalRecoveryAction action = recoveryStore.createAction(context.workspaceId(), drift == null ? null : drift.id(), actionType, referenceType, referenceId, context.actorId());
        try {
            RecoveryOutcome outcome = applyRecovery(context, action);
            if (outcome.completed() && drift != null) {
                recoveryStore.resolveDrift(context.workspaceId(), drift.id());
            }
            if (outcome.completed()) {
                return recoveryStore.markActionSucceeded(context.workspaceId(), action.id(), json(Map.of("status", "applied")));
            }
            return recoveryStore.markActionAwaitingPeerApply(context.workspaceId(), action.id(), json(Map.of("status", "replay_published", "completionBoundary", "peer_apply")));
        } catch (RuntimeException exception) {
            recoveryStore.markActionFailed(context.workspaceId(), action.id(), exception.getMessage());
            throw exception;
        }
    }

    private RecoveryOutcome applyRecovery(AccessContext context, RegionalRecoveryAction action) {
        return switch (action.actionType()) {
            case REPLAY_OWNERSHIP_TRANSFER -> {
                replayOwnershipTransfer(context, action.id());
                yield RecoveryOutcome.awaitingPeerApply();
            }
            case REDRIVE_DELAYED_PROVIDER_CALLBACK -> {
                providerCallbackService.redriveDelayedCallback(context, UUID.fromString(action.referenceId()));
                yield RecoveryOutcome.complete();
            }
            case REPLAY_INVESTIGATION_SNAPSHOT -> {
                investigationIndexingService.replayRegionalSnapshot(context.workspaceId(), action.referenceType(), UUID.fromString(action.referenceId()), action.id());
                yield RecoveryOutcome.awaitingPeerApply();
            }
            case REPLAY_PROVIDER_SUMMARY_SNAPSHOT -> {
                reportingService.replayProviderSummarySnapshot(context.workspaceId(), action.id());
                yield RecoveryOutcome.awaitingPeerApply();
            }
            default -> throw new BadRequestException("actionType is invalid.");
        };
    }

    private void replayOwnershipTransfer(AccessContext context, UUID recoveryActionId) {
        List<WorkspaceRegionFailoverEvent> events = regionService.failoverEvents(context.workspaceId());
        if (events.isEmpty()) {
            throw new BadRequestException("No failover event is available to replay.");
        }
        WorkspaceRegionFailoverEvent latest = events.get(0);
        regionReplicationService.publishOwnershipTransfer(
                context.workspaceId(),
                latest.fromRegion(),
                latest.toRegion(),
                latest.priorEpoch(),
                latest.newEpoch(),
                latest.triggerMode(),
                context.actorId(),
                recoveryActionId);
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private String json(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new BadRequestException("regional recovery details could not be serialized.");
        }
    }

    private static String requireAction(String actionType) {
        if (actionType == null || actionType.isBlank() || !List.of(
                REPLAY_OWNERSHIP_TRANSFER,
                REDRIVE_DELAYED_PROVIDER_CALLBACK,
                REPLAY_INVESTIGATION_SNAPSHOT,
                REPLAY_PROVIDER_SUMMARY_SNAPSHOT).contains(actionType.trim())) {
            throw new BadRequestException("actionType is invalid.");
        }
        return actionType.trim();
    }

    public record RecoveryCommand(UUID driftRecordId, String actionType, String referenceType, String referenceId) {
    }

    private record RecoveryOutcome(boolean completed) {
        private static RecoveryOutcome complete() {
            return new RecoveryOutcome(true);
        }

        private static RecoveryOutcome awaitingPeerApply() {
            return new RecoveryOutcome(false);
        }
    }
}
