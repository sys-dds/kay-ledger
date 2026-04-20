package com.kayledger.api.merchantevents.application;

import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.merchantevents.model.MerchantFinanceEndpoint;
import com.kayledger.api.merchantevents.model.MerchantFinanceEvent;
import com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery;
import com.kayledger.api.merchantevents.store.MerchantFinanceEventStore;
import com.kayledger.api.merchantevents.store.MerchantFinanceEventStore.DeliveryRuntimeSummary;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.messaging.application.OutboxService;

@Service
@EnableConfigurationProperties(MerchantFinanceDeliveryProperties.class)
public class MerchantFinanceEventService {

    public static final int ENVELOPE_VERSION = 1;

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
    private final MerchantFinanceDeliveryProperties deliveryProperties;
    private final HttpClient httpClient;
    private final AtomicLong deliveryClaimSequence = new AtomicLong();

    public MerchantFinanceEventService(
            MerchantFinanceEventStore merchantFinanceEventStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            OutboxService outboxService,
            ObjectMapper objectMapper,
            MerchantFinanceDeliveryProperties deliveryProperties) {
        this.merchantFinanceEventStore = merchantFinanceEventStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
        this.deliveryProperties = deliveryProperties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(deliveryProperties.getTimeoutSeconds()))
                .build();
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

    public DeliveryRuntimeSummary runtimeSummary(AccessContext context) {
        requireRead(context);
        return merchantFinanceEventStore.runtimeSummary(context.workspaceId());
    }

    @Transactional
    public MerchantFinanceEvent emit(UUID workspaceId, UUID providerProfileId, String currencyCode, UUID accountingPeriodId, String eventType, String sourceType, UUID sourceId, Map<String, Object> payload) {
        return emit(workspaceId, providerProfileId, currencyCode, accountingPeriodId, eventType, sourceType, sourceId, payload, Instant.now());
    }

    @Transactional
    public MerchantFinanceEvent emit(UUID workspaceId, UUID providerProfileId, String currencyCode, UUID accountingPeriodId, String eventType, String sourceType, UUID sourceId, Map<String, Object> payload, Instant occurredAt) {
        Instant domainOccurredAt = occurredAt == null ? Instant.now() : occurredAt;
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
                domainOccurredAt);
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
        return merchantFinanceEventStore.redriveDelivery(context.workspaceId(), deliveryId);
    }

    public MerchantFinanceEventDelivery recordDeliveryAttempt(UUID workspaceId, UUID deliveryId, boolean succeeded, Integer responseStatus, String responseBody) {
        return merchantFinanceEventStore.recordAttempt(workspaceId, deliveryId, succeeded ? "SUCCEEDED" : "FAILED", responseStatus, responseBody);
    }

    public int processDueDeliveries() {
        return processDueDeliveriesForWorkspace(null, "scheduler");
    }

    public int processDueDeliveries(AccessContext context) {
        requireWrite(context, "merchant finance event delivery processing");
        return processDueDeliveriesForWorkspace(context.workspaceId(), "workspace:" + context.workspaceId() + ":" + context.actorId());
    }

    @Transactional
    public DeliveryRuntimeMutationResult pauseRuntime(AccessContext context, String reason) {
        requireWrite(context, "merchant finance delivery runtime pause");
        merchantFinanceEventStore.setRuntimePaused(context.workspaceId(), context.actorId(), true, reason == null || reason.isBlank() ? "Paused by finance operator." : reason.trim());
        return new DeliveryRuntimeMutationResult(0);
    }

    @Transactional
    public DeliveryRuntimeMutationResult resumeRuntime(AccessContext context) {
        requireWrite(context, "merchant finance delivery runtime resume");
        merchantFinanceEventStore.setRuntimePaused(context.workspaceId(), context.actorId(), false, null);
        return new DeliveryRuntimeMutationResult(0);
    }

    @Transactional
    public DeliveryRuntimeMutationResult reclaimStaleClaims(AccessContext context) {
        requireWrite(context, "merchant finance delivery stale claim reclaim");
        return new DeliveryRuntimeMutationResult(merchantFinanceEventStore.reclaimStaleClaims(context.workspaceId()));
    }

    @Transactional
    public DeliveryRuntimeMutationResult requeueParked(AccessContext context) {
        requireWrite(context, "merchant finance delivery parked requeue");
        return new DeliveryRuntimeMutationResult(merchantFinanceEventStore.requeueByStatus(context.workspaceId(), "PARKED"));
    }

    @Transactional
    public DeliveryRuntimeMutationResult requeueFailed(AccessContext context) {
        requireWrite(context, "merchant finance delivery failed requeue");
        return new DeliveryRuntimeMutationResult(merchantFinanceEventStore.requeueByStatus(context.workspaceId(), "FAILED"));
    }

    private int processDueDeliveriesForWorkspace(UUID workspaceId, String ownerPrefix) {
        String claimOwner = ownerPrefix + ":" + java.lang.management.ManagementFactory.getRuntimeMXBean().getName() + ":" + deliveryClaimSequence.incrementAndGet();
        int processed = 0;
        for (var work : merchantFinanceEventStore.claimDueDeliveries(
                workspaceId,
                claimOwner,
                deliveryProperties.getBatchSize(),
                deliveryProperties.getMaxAttempts(),
                deliveryProperties.getLeaseSeconds())) {
            deliver(work);
            processed++;
        }
        return processed;
    }

    private void deliver(MerchantFinanceEventStore.DeliveryWork work) {
        Instant attemptedAt = Instant.now();
        String requestBody = deliveryPayload(work, attemptedAt);
        String requestSignature = signature(work.signingSecretRef(), requestBody);
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(work.endpointUrl()))
                    .timeout(Duration.ofSeconds(deliveryProperties.getTimeoutSeconds()))
                    .header("Content-Type", "application/json")
                    .header("X-Kay-Ledger-Event-Id", work.delivery().merchantFinanceEventId().toString())
                    .header("X-Kay-Ledger-Event-Type", work.eventType())
                    .header("X-Kay-Ledger-Delivery-Id", work.delivery().id().toString())
                    .header("X-Kay-Ledger-Dedupe-Key", work.delivery().dedupeKey())
                    .header("X-Kay-Ledger-Envelope-Version", Integer.toString(ENVELOPE_VERSION))
                    .header("X-Kay-Ledger-Attempted-At", attemptedAt.toString())
                    .header("X-Kay-Ledger-Signature", requestSignature)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            String body = snippet(response.body());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                merchantFinanceEventStore.recordDeliverySuccess(work.delivery().workspaceId(), work.delivery().id(), response.statusCode(), body);
            } else {
                merchantFinanceEventStore.recordDeliveryFailure(work.delivery().workspaceId(), work.delivery().id(), deliveryProperties.getMaxAttempts(), deliveryProperties.getBackoffSeconds(), response.statusCode(), body, "HTTP " + response.statusCode());
            }
        } catch (Exception exception) {
            merchantFinanceEventStore.recordDeliveryFailure(work.delivery().workspaceId(), work.delivery().id(), deliveryProperties.getMaxAttempts(), deliveryProperties.getBackoffSeconds(), null, null, failureReason(exception));
        }
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

    private String deliveryPayload(MerchantFinanceEventStore.DeliveryWork work, Instant attemptedAt) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("envelopeVersion", ENVELOPE_VERSION);
        envelope.put("eventId", work.delivery().merchantFinanceEventId());
        envelope.put("deliveryId", work.delivery().id());
        envelope.put("eventType", work.eventType());
        envelope.put("sourceReferenceType", work.sourceReferenceType());
        envelope.put("sourceReferenceId", work.sourceReferenceId());
        envelope.put("eventKey", work.eventKey());
        envelope.put("occurredAt", work.occurredAt());
        envelope.put("attemptedAt", attemptedAt);
        envelope.put("payload", payloadObject(work.payloadJson()));
        return json(envelope);
    }

    private Object payloadObject(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson == null ? "{}" : payloadJson, Object.class);
        } catch (Exception exception) {
            throw new BadRequestException("Merchant finance event payload could not be read.");
        }
    }

    private String snippet(String value) {
        if (value == null) {
            return null;
        }
        return value.length() > deliveryProperties.getResponseBodyMaxChars()
                ? value.substring(0, deliveryProperties.getResponseBodyMaxChars())
                : value;
    }

    private static String failureReason(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    private static String signature(String secretRef, String eventKey, String payloadJson) {
        return signature(secretRef, eventKey + "." + payloadJson);
    }

    private static String signature(String secretRef, String signedBody) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretRef.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal(signedBody.getBytes(StandardCharsets.UTF_8));
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

    public record DeliveryRuntimeMutationResult(int affectedCount) {
    }
}
