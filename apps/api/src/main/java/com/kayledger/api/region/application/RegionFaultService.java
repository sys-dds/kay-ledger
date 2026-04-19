package com.kayledger.api.region.application;

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
import com.kayledger.api.region.model.RegionChaosFault;
import com.kayledger.api.region.store.RegionFaultStore;
import com.kayledger.api.shared.api.BadRequestException;

@Service
public class RegionFaultService {

    public static final String DELAY_PROVIDER_CALLBACK_APPLY = "DELAY_PROVIDER_CALLBACK_APPLY";
    public static final String DROP_PROVIDER_CALLBACK_APPLY = "DROP_PROVIDER_CALLBACK_APPLY";
    public static final String DUPLICATE_PROVIDER_CALLBACK_APPLY = "DUPLICATE_PROVIDER_CALLBACK_APPLY";
    public static final String OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY = "OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY";
    public static final String REGIONAL_REPLICATION_PUBLISH_BLOCK = "REGIONAL_REPLICATION_PUBLISH_BLOCK";
    public static final String REGIONAL_REPLICATION_APPLY_BLOCK = "REGIONAL_REPLICATION_APPLY_BLOCK";
    public static final String REGIONAL_REPLICATION_PUBLISH_DELAY = "REGIONAL_REPLICATION_PUBLISH_DELAY";
    public static final String REGIONAL_REPLICATION_APPLY_DELAY = "REGIONAL_REPLICATION_APPLY_DELAY";

    private static final List<String> TYPES = List.of(
            DELAY_PROVIDER_CALLBACK_APPLY,
            DROP_PROVIDER_CALLBACK_APPLY,
            DUPLICATE_PROVIDER_CALLBACK_APPLY,
            OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY,
            REGIONAL_REPLICATION_PUBLISH_BLOCK,
            REGIONAL_REPLICATION_APPLY_BLOCK,
            REGIONAL_REPLICATION_PUBLISH_DELAY,
            REGIONAL_REPLICATION_APPLY_DELAY);

    private final RegionFaultStore regionFaultStore;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public RegionFaultService(RegionFaultStore regionFaultStore, AccessPolicy accessPolicy, ObjectMapper objectMapper) {
        this.regionFaultStore = regionFaultStore;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public RegionChaosFault inject(AccessContext context, FaultCommand command) {
        requireWrite(context);
        String type = requireType(command == null ? null : command.faultType());
        String scope = command == null || command.scope() == null || command.scope().isBlank() ? "WORKSPACE" : command.scope().trim();
        if (!List.of("WORKSPACE", "REGION").contains(scope)) {
            throw new BadRequestException("scope is invalid.");
        }
        String storedScope = "REGION".equals(scope) ? "WORKSPACE" : scope;
        return regionFaultStore.create(context.workspaceId(), type, storedScope, json(command == null ? null : command.parameters()), command == null ? null : command.reason(), context.actorId());
    }

    @Transactional
    public RegionChaosFault clear(AccessContext context, UUID faultId) {
        requireWrite(context);
        return regionFaultStore.clear(context.workspaceId(), faultId);
    }

    public List<RegionChaosFault> active(AccessContext context) {
        requireRead(context);
        return regionFaultStore.active(context.workspaceId());
    }

    public boolean active(UUID workspaceId, String faultType) {
        return regionFaultStore.activeFault(workspaceId, faultType).isPresent();
    }

    public void simulateDelayIfActive(UUID workspaceId, String faultType) {
        if (active(workspaceId, faultType)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private void requireWrite(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private String json(Map<String, Object> parameters) {
        try {
            return objectMapper.writeValueAsString(parameters == null ? Map.of() : parameters);
        } catch (Exception exception) {
            throw new BadRequestException("fault parameters could not be serialized.");
        }
    }

    private static String requireType(String value) {
        if (value == null || value.isBlank() || !TYPES.contains(value.trim())) {
            throw new BadRequestException("faultType is invalid.");
        }
        return value.trim();
    }

    public record FaultCommand(String faultType, String scope, Map<String, Object> parameters, String reason) {
    }
}
