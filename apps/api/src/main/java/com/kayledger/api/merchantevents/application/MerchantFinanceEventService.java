package com.kayledger.api.merchantevents.application;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.merchantevents.model.MerchantFinanceEndpoint;
import com.kayledger.api.merchantevents.model.MerchantFinanceEvent;
import com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery;
import com.kayledger.api.merchantevents.store.MerchantFinanceEventStore;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.messaging.application.OutboxService;

@Service
public class MerchantFinanceEventService {

    public static final String EVENT_FINALIZED_STATEMENT_AVAILABLE = "FINALIZED_STATEMENT_AVAILABLE";
    public static final String EVENT_ACCOUNTING_PERIOD_REOPENED = "ACCOUNTING_PERIOD_REOPENED";
    public static final String EVENT_RECONCILIATION_RUN_MISMATCHED = "RECONCILIATION_RUN_MISMATCHED";
    public static final String EVENT_RECONCILIATION_IMPORT_RESOLVED = "RECONCILIATION_IMPORT_RESOLVED";
    public static final String EVENT_APPROVAL_GRANTED = "APPROVAL_GRANTED";
    public static final String EVENT_APPROVAL_REJECTED = "APPROVAL_REJECTED";
    public static final String EVENT_PAYOUT_SUCCEEDED = "PAYOUT_SUCCEEDED";
    public static final String EVENT_REFUND_SUCCEEDED = "REFUND_SUCCEEDED";
    public static final String EVENT_DISPUTE_OPENED = "DISPUTE_OPENED";
    public static final String EVENT_DISPUTE_RESOLVED = "DISPUTE_RESOLVED";

    private final MerchantFinanceEventStore merchantFinanceEventStore;
    private final AccessPolicy accessPolicy;
    private final RegionService regionService;
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    public MerchantFinanceEventService(
            MerchantFinanceEventStore merchantFinanceEventStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            OutboxService outboxService,
            ObjectMapper objectMapper) {
        this.merchantFinanceEventStore = merchantFinanceEventStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public MerchantFinanceEndpoint configureEndpoint(AccessContext context, ConfigureEndpointCommand command) {
        requireWrite(context, "merchant finance endpoint configuration");
        if (command == null) {
            throw new BadRequestException("merchant finance endpoint request body is required.");
        }
        return merchantFinanceEventStore.createEndpoint(
                context.workspaceId(),
                command.providerProfileId(),
                requireText(command.endpointUrl(), "endpointUrl"),
                requireText(command.signingSecretRef(), "signingSecretRef"),
                command.eventTypes() == null ? new String[0] : command.eventTypes(),
                context.actorId());
    }

    public List<MerchantFinanceEndpoint> listEndpoints(AccessContext context) {
        requireRead(context);
        return merchantFinanceEventStore.listEndpoints(context.workspaceId());
    }

    public List<MerchantFinanceEventDelivery> listDeliveries(AccessContext context) {
        requireRead(context);
        return merchantFinanceEventStore.listDeliveries(context.workspaceId());
    }

    @Transactional
    public MerchantFinanceEvent emit(UUID workspaceId, UUID providerProfileId, String currencyCode, UUID accountingPeriodId, String eventType, String sourceType, UUID sourceId, Map<String, Object> payload) {
        if (workspaceId == null || eventType == null || sourceType == null || sourceId == null) {
            throw new BadRequestException("Merchant finance event source is required.");
        }
        Map<String, Object> safePayload = payload == null ? Map.of() : payload;
        String eventKey = eventType + ":" + sourceType + ":" + sourceId;
        MerchantFinanceEvent event = merchantFinanceEventStore.upsertEvent(
                workspaceId,
                providerProfileId,
                currencyCode,
                accountingPeriodId,
                eventType,
                sourceType,
                sourceId,
                json(safePayload),
                eventKey,
                Instant.now());
        for (MerchantFinanceEndpoint endpoint : merchantFinanceEventStore.matchingEndpoints(workspaceId, providerProfileId, eventType)) {
            String dedupeKey = event.id() + ":" + endpoint.id();
            merchantFinanceEventStore.upsertDelivery(workspaceId, event.id(), endpoint.id(), dedupeKey, signature(endpoint.signingSecretRef(), event.eventKey(), event.payloadJson()));
        }
        Map<String, Object> outboxPayload = new LinkedHashMap<>();
        outboxPayload.put("merchantFinanceEventId", event.id());
        outboxPayload.put("eventType", event.eventType());
        outboxPayload.put("sourceReferenceType", event.sourceReferenceType());
        outboxPayload.put("sourceReferenceId", event.sourceReferenceId());
        outboxService.append(workspaceId, "MERCHANT_FINANCE_EVENT", event.id(), "merchant_finance_event.created", event.eventKey(), outboxPayload);
        return event;
    }

    @Transactional
    public MerchantFinanceEventDelivery redriveDelivery(AccessContext context, UUID deliveryId) {
        requireWrite(context, "merchant finance event delivery redrive");
        if (deliveryId == null) {
            throw new BadRequestException("deliveryId is required.");
        }
        return merchantFinanceEventStore.recordAttempt(context.workspaceId(), deliveryId, "PENDING", null, "Delivery redrive requested.");
    }

    public MerchantFinanceEventDelivery recordDeliveryAttempt(UUID workspaceId, UUID deliveryId, boolean succeeded, Integer responseStatus, String responseBody) {
        return merchantFinanceEventStore.recordAttempt(workspaceId, deliveryId, succeeded ? "SUCCEEDED" : "FAILED", responseStatus, responseBody);
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireWrite(AccessContext context, String operation) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
        regionService.requireOwnedForWrite(context, operation);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new BadRequestException("Merchant finance event payload could not be serialized.");
        }
    }

    private static String signature(String secretRef, String eventKey, String payloadJson) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretRef.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal((eventKey + "." + payloadJson).getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (Exception exception) {
            throw new BadRequestException("Merchant finance event signature could not be generated.");
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(fieldName + " is required.");
        }
        return value.trim();
    }

    public record ConfigureEndpointCommand(UUID providerProfileId, String endpointUrl, String signingSecretRef, String[] eventTypes) {
    }
}
