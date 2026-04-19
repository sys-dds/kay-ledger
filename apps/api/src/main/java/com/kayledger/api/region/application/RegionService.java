package com.kayledger.api.region.application;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.model.WorkspaceRegionFailoverEvent;
import com.kayledger.api.region.model.WorkspaceRegionOwnership;
import com.kayledger.api.region.store.RegionStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;

@Service
public class RegionService {

    public static final String MANUAL_OPERATOR = "MANUAL_OPERATOR";
    public static final String SIMULATED_REGION_FAILOVER = "SIMULATED_REGION_FAILOVER";
    public static final String MANUAL_FAILBACK = "MANUAL_FAILBACK";

    private final RegionStore regionStore;
    private final RegionProperties regionProperties;
    private final AccessPolicy accessPolicy;
    private final RegionReplicationService regionReplicationService;

    public RegionService(RegionStore regionStore, RegionProperties regionProperties, AccessPolicy accessPolicy, RegionReplicationService regionReplicationService) {
        this.regionStore = regionStore;
        this.regionProperties = regionProperties;
        this.accessPolicy = accessPolicy;
        this.regionReplicationService = regionReplicationService;
    }

    public String localRegionId() {
        return requireRegion(regionProperties.getLocalRegionId(), "local region id");
    }

    public List<String> peerRegionIds() {
        return List.copyOf(regionProperties.getPeerRegionIds());
    }

    public RegionTopology topology(AccessContext context) {
        requireRegionRead(context);
        return new RegionTopology(
                localRegionId(),
                peerRegionIds(),
                regionProperties.getActiveRegionLabel(),
                regionProperties.getHomeRegionMode(),
                regionProperties.isReplicationProducerEnabled(),
                regionProperties.isReplicationConsumerEnabled());
    }

    @Transactional
    public WorkspaceRegionOwnership ensureWorkspaceOwnership(UUID workspaceId) {
        return regionStore.ensureOwnership(workspaceId, localRegionId());
    }

    @Transactional
    public WorkspaceRegionOwnership requireOwnedForWrite(AccessContext context, String mutationName) {
        return requireOwnedForWrite(context.workspaceId(), mutationName);
    }

    @Transactional
    public WorkspaceRegionOwnership requireOwnedForWrite(UUID workspaceId, String mutationName) {
        WorkspaceRegionOwnership ownership = regionStore.findOwnership(workspaceId)
                .orElseThrow(() -> new ForbiddenException("Workspace has no region ownership record for " + mutationName + "."));
        requireActiveOwnership(ownership, mutationName);
        if (!localRegionId().equals(ownership.homeRegion())) {
            throw new ForbiddenException("Workspace writes for " + mutationName + " are owned by region " + ownership.homeRegion() + ".");
        }
        return ownership;
    }

    @Transactional
    public WorkspaceRegionOwnership requireOwnedForWrite(UUID workspaceId, long expectedOwnershipEpoch, String mutationName) {
        WorkspaceRegionOwnership ownership = requireOwnedForWrite(workspaceId, mutationName);
        if (ownership.ownershipEpoch() != expectedOwnershipEpoch) {
            throw new ForbiddenException("Workspace ownership epoch is stale for " + mutationName + ".");
        }
        return ownership;
    }

    @Transactional(readOnly = true)
    public WorkspaceRegionOwnership ownership(UUID workspaceId) {
        return regionStore.findOwnership(workspaceId)
                .orElseThrow(() -> new BadRequestException("Workspace has no region ownership record."));
    }

    @Transactional
    public WorkspaceRegionOwnership ownership(AccessContext context) {
        requireRegionRead(context);
        return ownership(context.workspaceId());
    }

    @Transactional(readOnly = true)
    public boolean isLocalOwner(UUID workspaceId) {
        return regionStore.findOwnership(workspaceId)
                .map(ownership -> localRegionId().equals(ownership.homeRegion()) && "ACTIVE".equals(ownership.status()))
                .orElse(false);
    }

    @Transactional
    public WorkspaceRegionFailoverEvent transferOwnership(AccessContext context, String toRegion, String triggerMode) {
        requireRegionWrite(context);
        String targetRegion = requireRegion(toRegion, "target region");
        requireKnownPeerRegion(targetRegion);
        String trigger = requireTriggerMode(triggerMode);
        WorkspaceRegionOwnership existing = ownership(context.workspaceId());
        if (existing.homeRegion().equals(targetRegion)) {
            throw new BadRequestException("Workspace is already owned by region " + targetRegion + ".");
        }
        WorkspaceRegionOwnership locked = regionStore.lockOwnership(context.workspaceId());
        if (!localRegionId().equals(locked.homeRegion())) {
            throw new ForbiddenException("Workspace ownership transfer must be started from the current home region.");
        }
        long newEpoch = locked.ownershipEpoch() + 1;
        regionStore.transferOwnership(context.workspaceId(), targetRegion, newEpoch);
        WorkspaceRegionFailoverEvent event = regionStore.recordFailover(context.workspaceId(), locked.homeRegion(), targetRegion, locked.ownershipEpoch(), newEpoch, trigger, context.actorId());
        regionReplicationService.publishOwnershipTransfer(context.workspaceId(), locked.homeRegion(), targetRegion, locked.ownershipEpoch(), newEpoch, trigger, context.actorId());
        return event;
    }

    @Transactional(readOnly = true)
    public List<WorkspaceRegionFailoverEvent> failoverEvents(UUID workspaceId) {
        return regionStore.listFailoverEvents(workspaceId);
    }

    private static void requireActiveOwnership(WorkspaceRegionOwnership ownership, String mutationName) {
        if (!"ACTIVE".equals(ownership.status())) {
            throw new ForbiddenException("Workspace ownership is not active for " + mutationName + ".");
        }
    }

    private void requireRegionRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireRegionWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private static String requireRegion(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireTriggerMode(String triggerMode) {
        String value = triggerMode == null || triggerMode.isBlank() ? MANUAL_OPERATOR : triggerMode.trim();
        if (!List.of(MANUAL_OPERATOR, SIMULATED_REGION_FAILOVER, MANUAL_FAILBACK).contains(value)) {
            throw new BadRequestException("triggerMode is invalid.");
        }
        return value;
    }

    private void requireKnownPeerRegion(String targetRegion) {
        if (!peerRegionIds().contains(targetRegion)) {
            throw new BadRequestException("target region is not a configured peer region.");
        }
    }

    public record RegionTopology(
            String localRegionId,
            List<String> peerRegionIds,
            String activeRegionLabel,
            String homeRegionMode,
            boolean replicationProducerEnabled,
            boolean replicationConsumerEnabled) {
    }
}
