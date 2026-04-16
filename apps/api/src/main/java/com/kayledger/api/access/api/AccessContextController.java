package com.kayledger.api.access.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessContextResolver;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;

@RestController
@RequestMapping("/api/access")
public class AccessContextController {

    private final AccessContextResolver accessContextResolver;
    private final AccessPolicy accessPolicy;

    public AccessContextController(AccessContextResolver accessContextResolver, AccessPolicy accessPolicy) {
        this.accessContextResolver = accessContextResolver;
        this.accessPolicy = accessPolicy;
    }

    @GetMapping("/context")
    AccessContext current(
            @RequestHeader(value = "X-Workspace-Slug", required = false) String workspaceSlug,
            @RequestHeader(value = "X-Actor-Key", required = false) String actorKey) {
        AccessContext context = accessContextResolver.resolveWorkspace(workspaceSlug, actorKey);
        accessPolicy.requireScope(context, AccessScope.ACCESS_CONTEXT_READ);
        return context;
    }
}
