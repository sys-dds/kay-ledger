package com.kayledger.api.region.api;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.application.RegionService.RegionTopology;
import com.kayledger.api.region.model.RegionReplicationCheckpoint;
import com.kayledger.api.region.model.WorkspaceRegionFailoverEvent;
import com.kayledger.api.region.model.WorkspaceRegionOwnership;

@RestController
@RequestMapping("/api/region")
public class RegionTopologyController {

    private final AccessContextResolver accessContextResolver;
    private final RegionService regionService;
    private final RegionReplicationService regionReplicationService;

    public RegionTopologyController(AccessContextResolver accessContextResolver, RegionService regionService, RegionReplicationService regionReplicationService) {
        this.accessContextResolver = accessContextResolver;
        this.regionService = regionService;
        this.regionReplicationService = regionReplicationService;
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

    public record FailoverRequest(String toRegion, String triggerMode) {
    }
}
