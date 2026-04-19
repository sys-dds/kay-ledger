package com.kayledger.api.region.api;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionFaultService.FaultCommand;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.application.RegionService.RegionTopology;
import com.kayledger.api.region.model.RegionChaosFault;
import com.kayledger.api.region.model.RegionReplicationCheckpoint;
import com.kayledger.api.region.model.WorkspaceRegionFailoverEvent;
import com.kayledger.api.region.model.WorkspaceRegionOwnership;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService.RecoveryCommand;
import com.kayledger.api.region.recovery.model.RegionalDriftRecord;
import com.kayledger.api.region.recovery.model.RegionalRecoveryAction;

@RestController
@RequestMapping("/api/region")
public class RegionTopologyController {

    private final AccessContextResolver accessContextResolver;
    private final RegionService regionService;
    private final RegionReplicationService regionReplicationService;
    private final RegionFaultService regionFaultService;
    private final RegionalRecoveryService regionalRecoveryService;

    public RegionTopologyController(AccessContextResolver accessContextResolver, RegionService regionService, RegionReplicationService regionReplicationService, RegionFaultService regionFaultService, RegionalRecoveryService regionalRecoveryService) {
        this.accessContextResolver = accessContextResolver;
        this.regionService = regionService;
        this.regionReplicationService = regionReplicationService;
        this.regionFaultService = regionFaultService;
        this.regionalRecoveryService = regionalRecoveryService;
    }

    @GetMapping("/topology")
    RegionTopology topology(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionService.topology(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/workspaces/current/ownership")
    WorkspaceRegionOwnership ownership(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionService.ownership(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/replication/checkpoints")
    List<RegionReplicationCheckpoint> checkpoints(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        regionService.topology(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
        return regionReplicationService.checkpoints();
    }

    @PostMapping("/workspaces/current/failover")
    WorkspaceRegionFailoverEvent failover(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody FailoverRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return regionService.transferOwnership(context, request == null ? null : request.toRegion(), request == null ? null : request.triggerMode());
    }

    @GetMapping("/workspaces/current/failover-history")
    List<WorkspaceRegionFailoverEvent> failoverHistory(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        regionService.topology(context);
        return regionService.failoverEvents(context.workspaceId());
    }

    @PostMapping("/workspaces/current/failover-drill")
    WorkspaceRegionFailoverEvent failoverDrill(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody FailoverRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return regionService.transferOwnership(context, request == null ? null : request.toRegion(), RegionService.SIMULATED_REGION_FAILOVER);
    }

    @PostMapping("/workspaces/current/failback-drill")
    WorkspaceRegionFailoverEvent failbackDrill(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody FailoverRequest request) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return regionService.transferOwnership(context, request == null ? null : request.toRegion(), RegionService.MANUAL_FAILBACK);
    }

    @GetMapping("/faults/active")
    List<RegionChaosFault> activeFaults(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionFaultService.active(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/faults")
    RegionChaosFault injectFault(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody FaultCommand command) {
        return regionFaultService.inject(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), command);
    }

    @PostMapping("/faults/{faultId}/clear")
    RegionChaosFault clearFault(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @org.springframework.web.bind.annotation.PathVariable java.util.UUID faultId) {
        return regionFaultService.clear(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), faultId);
    }

    @GetMapping("/recovery/drift")
    List<RegionalDriftRecord> drift(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionalRecoveryService.listDrift(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/recovery/drift/unresolved")
    List<RegionalDriftRecord> unresolvedDrift(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionalRecoveryService.listUnresolvedDrift(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/recovery/drift/scan")
    List<RegionalDriftRecord> scanDrift(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionalRecoveryService.scan(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @PostMapping("/recovery/actions")
    RegionalRecoveryAction requestRecovery(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey,
            @RequestBody RecoveryCommand command) {
        return regionalRecoveryService.requestRecovery(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey), command);
    }

    @GetMapping("/recovery/actions")
    List<RegionalRecoveryAction> recoveryActions(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        return regionalRecoveryService.listActions(accessContextResolver.resolveWorkspace(workspaceSlug, actorKey));
    }

    @GetMapping("/recovery/history")
    Map<String, Object> recoveryHistory(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        return Map.of(
                "failoverEvents", regionService.failoverEvents(context.workspaceId()),
                "recoveryActions", regionalRecoveryService.listActions(context));
    }

    public record FailoverRequest(String toRegion, String triggerMode) {
    }
}
