package com.kayledger.api.access.api;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.application.OperatorContext;
import com.kayledger.api.workspace.Workspace;
import com.kayledger.api.workspace.application.WorkspaceService;

@RestController
@RequestMapping("/api/operator")
public class OperatorController {

    private final AccessContextResolver accessContextResolver;
    private final AccessPolicy accessPolicy;
    private final WorkspaceService workspaceService;

    public OperatorController(
            AccessContextResolver accessContextResolver,
            AccessPolicy accessPolicy,
            WorkspaceService workspaceService) {
        this.accessContextResolver = accessContextResolver;
        this.accessPolicy = accessPolicy;
        this.workspaceService = workspaceService;
    }

    @GetMapping("/workspaces")
    List<Workspace> listWorkspaces(@RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        OperatorContext context = accessContextResolver.resolveOperator(actorKey);
        accessPolicy.requireOperator(context);
        return workspaceService.listForOperator();
    }
}
