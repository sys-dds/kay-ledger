package com.kayledger.api.provider.application;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.model.ProviderConfig;
import com.kayledger.api.provider.store.ProviderStore;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class ProviderCallbackService {

    private static final Set<String> CALLBACK_TYPES = Set.of(
            "PAYMENT_AUTHORIZED",
            "PAYMENT_CAPTURED",
            "PAYMENT_SETTLED",
            "PAYMENT_FAILED",
            "REFUND_SUCCEEDED",
            "REFUND_FAILED",
            "PAYOUT_SUCCEEDED",
            "PAYOUT_FAILED");
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final ProviderStore providerStore;
    private final PaymentService paymentService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;
    private final InvestigationIndexingService investigationIndexingService;
    private final RegionFaultService regionFaultService;

    public ProviderCallbackService(ProviderStore providerStore, PaymentService paymentService, AccessPolicy accessPolicy, ObjectMapper objectMapper, InvestigationIndexingService investigationIndexingService, RegionFaultService regionFaultService) {
        this.providerStore = providerStore;
        this.paymentService = paymentService;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
        this.investigationIndexingService = investigationIndexingService;
        this.regionFaultService = regionFaultService;
    }

    @Transactional
    public ProviderConfig createConfig(AccessContext context, CreateProviderConfigCommand command) {
        requireProviderAdmin(context);
        return providerStore.createConfig(
                context.workspaceId(),
                requireText(command.providerKey(), "providerKey"),
                requireText(command.displayName(), "displayName"),
                requireText(command.signingSecret(), "signingSecret"),
                callbackToken(command.callbackToken()));
    }

    public List<ProviderCallback> listCallbacks(AccessContext context) {
        requireProviderRead(context);
        return providerStore.listCallbacks(context.workspaceId());
    }

    public List<ProviderCallback> listDelayedCallbacks(AccessContext context) {
        requireProviderRead(context);
        return providerStore.listDelayedCallbacks(context.workspaceId());
    }

    public Map<String, Object> investigationReference(AccessContext context, UUID callbackId) {
        requireProviderRead(context);
        ProviderCallback callback = providerStore.findCallback(context.workspaceId(), callbackId)
                .orElseThrow(() -> new NotFoundException("Provider callback was not found."));
        return Map.of(
                "referenceType", "PROVIDER_CALLBACK",
                "referenceId", callback.id(),
                "providerEventId", callback.providerEventId(),
                "businessReferenceType", callback.businessReferenceType(),
                "businessReferenceId", callback.businessReferenceId());
    }

    public ProviderCallback ingestExternal(String callbackToken, String signatureHeader, byte[] rawPayload) {
        ProviderConfig config = providerStore.findConfigByCallbackToken(requireText(callbackToken, "callbackToken"))
                .orElseThrow(() -> new NotFoundException("Provider config was not found."));
        if (rawPayload == null || rawPayload.length == 0) {
            throw new BadRequestException("request body is required.");
        }
        return ingest(config, signatureHeader, rawPayload);
    }

    public ProviderCallback ingestWorkspaceScoped(AccessContext context, String providerKey, String signatureHeader, byte[] rawPayload) {
        requireProviderAdmin(context);
        ProviderConfig config = providerStore.findConfig(context.workspaceId(), requireText(providerKey, "providerKey"))
                .orElseThrow(() -> new NotFoundException("Provider config was not found."));
        if (rawPayload == null || rawPayload.length == 0) {
            throw new BadRequestException("request body is required.");
        }
        return ingest(config, signatureHeader, rawPayload);
    }

    private ProviderCallback ingest(ProviderConfig config, String signatureHeader, byte[] rawPayload) {
        UUID workspaceId = config.workspaceId();
        ProviderCallbackCommand command = payload(rawPayload);
        if (!validSignature(config.signingSecret(), rawPayload, requireText(signatureHeader, "X-Provider-Signature"))) {
            throw new ForbiddenException("Provider callback signature is invalid.");
        }
        String payloadJson = new String(rawPayload, StandardCharsets.UTF_8);
        String callbackType = requireOneOf(command.callbackType(), CALLBACK_TYPES, "callbackType");
        String referenceType = referenceType(callbackType);
        UUID referenceId = requireId(command.businessReferenceId(), "businessReferenceId");
        String dedupeKey = config.providerKey() + ":" + requireText(command.providerEventId(), "providerEventId");
        ProviderCallback callback = providerStore.insertCallback(
                workspaceId,
                config.id(),
                config.providerKey(),
                command.providerEventId(),
                command.providerSequence(),
                callbackType,
                referenceType,
                referenceId,
                payloadJson,
                signatureHeader,
                true,
                dedupeKey)
                .orElseGet(() -> providerStore.findCallbackByDedupe(workspaceId, config.providerKey(), dedupeKey)
                        .orElseThrow(() -> new BadRequestException("Provider callback dedupe state could not be resolved.")));
        if (!"RECEIVED".equals(callback.processingStatus())) {
            return callback;
        }
        if (regionFaultService.active(workspaceId, RegionFaultService.DELAY_PROVIDER_CALLBACK_APPLY)) {
            ProviderCallback delayed = providerStore.markDelayedByDrill(workspaceId, callback.id());
            reindex(workspaceId, referenceType, referenceId, callback.id());
            return delayed;
        }
        if (regionFaultService.active(workspaceId, RegionFaultService.DROP_PROVIDER_CALLBACK_APPLY)) {
            ProviderCallback dropped = providerStore.markFailed(workspaceId, callback.id(), "Simulated dropped provider callback apply drill.");
            reindex(workspaceId, referenceType, referenceId, callback.id());
            return dropped;
        }
        if (regionFaultService.active(workspaceId, RegionFaultService.OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY)) {
            Long latest = providerStore.latestAppliedSequence(workspaceId, referenceType, referenceId);
            ProviderCallback ignored = latest != null && callback.providerSequence() != null && callback.providerSequence() <= latest
                    ? providerStore.markIgnoredOutOfOrder(workspaceId, callback.id())
                    : providerStore.markDelayedByDrill(workspaceId, callback.id());
            reindex(workspaceId, referenceType, referenceId, callback.id());
            return ignored;
        }
        Long latest = providerStore.latestAppliedSequence(workspaceId, referenceType, referenceId);
        if (latest != null && callback.providerSequence() != null && callback.providerSequence() <= latest) {
            ProviderCallback ignored = providerStore.markIgnoredOutOfOrder(workspaceId, callback.id());
            reindex(workspaceId, referenceType, referenceId, callback.id());
            return ignored;
        }
        try {
            applyTruth(workspaceId, callbackType, referenceId, amount(command), command.providerEventId());
            if (regionFaultService.active(workspaceId, RegionFaultService.DUPLICATE_PROVIDER_CALLBACK_APPLY)) {
                applyTruth(workspaceId, callbackType, referenceId, amount(command), command.providerEventId());
            }
            ProviderCallback applied = providerStore.markApplied(workspaceId, callback.id());
            reindex(workspaceId, referenceType, referenceId, callback.id());
            return applied;
        } catch (RuntimeException exception) {
            providerStore.markFailed(workspaceId, callback.id(), exception.getMessage());
            reindex(workspaceId, referenceType, referenceId, callback.id());
            throw new ProviderCallbackApplyException("Provider callback application failed; retry is required.", exception);
        }
    }

    @Transactional
    public ProviderCallback redriveDelayedCallback(AccessContext context, UUID callbackId) {
        requireProviderAdmin(context);
        ProviderCallback callback = providerStore.findCallback(context.workspaceId(), callbackId)
                .orElseThrow(() -> new NotFoundException("Provider callback was not found."));
        if (!"DELAYED_BY_DRILL".equals(callback.processingStatus())) {
            throw new BadRequestException("Provider callback is not delayed by a drill.");
        }
        return applyStoredCallback(callback);
    }

    @Transactional
    public ProviderCallback redriveDelayedCallback(UUID workspaceId, UUID callbackId) {
        ProviderCallback callback = providerStore.findCallback(workspaceId, callbackId)
                .orElseThrow(() -> new NotFoundException("Provider callback was not found."));
        if (!"DELAYED_BY_DRILL".equals(callback.processingStatus())) {
            throw new BadRequestException("Provider callback is not delayed by a drill.");
        }
        return applyStoredCallback(callback);
    }

    private ProviderCallback applyStoredCallback(ProviderCallback callback) {
        Long latest = providerStore.latestAppliedSequence(callback.workspaceId(), callback.businessReferenceType(), callback.businessReferenceId());
        if (latest != null && callback.providerSequence() != null && callback.providerSequence() <= latest) {
            ProviderCallback ignored = providerStore.markIgnoredOutOfOrder(callback.workspaceId(), callback.id());
            reindex(callback.workspaceId(), callback.businessReferenceType(), callback.businessReferenceId(), callback.id());
            return ignored;
        }
        try {
            ProviderCallbackCommand command = payload(callback.payloadJson().getBytes(StandardCharsets.UTF_8));
            applyTruth(callback.workspaceId(), callback.callbackType(), callback.businessReferenceId(), amount(command), callback.providerEventId());
            ProviderCallback applied = providerStore.markApplied(callback.workspaceId(), callback.id());
            reindex(callback.workspaceId(), callback.businessReferenceType(), callback.businessReferenceId(), callback.id());
            return applied;
        } catch (RuntimeException exception) {
            providerStore.markFailed(callback.workspaceId(), callback.id(), exception.getMessage());
            reindex(callback.workspaceId(), callback.businessReferenceType(), callback.businessReferenceId(), callback.id());
            throw new ProviderCallbackApplyException("Provider callback re-drive failed; retry is required.", exception);
        }
    }

    private void reindex(UUID workspaceId, String referenceType, UUID referenceId, UUID callbackId) {
        try {
            investigationIndexingService.indexReference(workspaceId, referenceType, referenceId);
            investigationIndexingService.indexReference(workspaceId, "PROVIDER_CALLBACK", callbackId);
        } catch (RuntimeException ignored) {
            // Operator search is an index target; callback truth stays durable in PostgreSQL.
        }
    }

    private void applyTruth(UUID workspaceId, String callbackType, UUID referenceId, long amountMinor, String externalReference) {
        switch (callbackType) {
            case "PAYMENT_AUTHORIZED" -> paymentService.applyProviderPaymentTruth(workspaceId, referenceId, "AUTHORIZED", amountMinor, externalReference);
            case "PAYMENT_CAPTURED" -> paymentService.applyProviderPaymentTruth(workspaceId, referenceId, "CAPTURED", amountMinor, externalReference);
            case "PAYMENT_SETTLED" -> paymentService.applyProviderPaymentTruth(workspaceId, referenceId, "SETTLED", amountMinor, externalReference);
            case "PAYMENT_FAILED" -> paymentService.applyProviderPaymentTruth(workspaceId, referenceId, "FAILED", amountMinor, externalReference);
            case "REFUND_SUCCEEDED" -> paymentService.applyProviderRefundTruth(workspaceId, referenceId, "SUCCEEDED", externalReference, null);
            case "REFUND_FAILED" -> paymentService.applyProviderRefundTruth(workspaceId, referenceId, "FAILED", externalReference, "provider reported failure");
            case "PAYOUT_SUCCEEDED" -> paymentService.applyProviderPayoutTruth(workspaceId, referenceId, "SUCCEEDED", externalReference, null);
            case "PAYOUT_FAILED" -> paymentService.applyProviderPayoutTruth(workspaceId, referenceId, "FAILED", externalReference, "provider reported failure");
            default -> throw new BadRequestException("callbackType is invalid.");
        }
    }

    private void requireProviderAdmin(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_WRITE);
    }

    private void requireProviderRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.PAYMENT_READ);
    }

    private ProviderCallbackCommand payload(byte[] rawPayload) {
        try {
            return objectMapper.readValue(rawPayload, ProviderCallbackCommand.class);
        } catch (IOException exception) {
            throw new BadRequestException("Provider callback payload could not be parsed.");
        }
    }

    private static String sign(String secret, byte[] payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload));
        } catch (Exception exception) {
            throw new BadRequestException("Provider signature could not be verified.");
        }
    }

    private static boolean validSignature(String secret, byte[] rawPayload, String actual) {
        return constantTimeEquals(sign(secret, rawPayload), actual);
    }

    private static boolean constantTimeEquals(String expected, String actual) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                actual.getBytes(StandardCharsets.UTF_8));
    }

    private static String callbackToken(String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested.trim();
        }
        byte[] token = new byte[32];
        SECURE_RANDOM.nextBytes(token);
        return HexFormat.of().formatHex(token);
    }

    private static String referenceType(String callbackType) {
        if (callbackType.startsWith("PAYMENT_")) {
            return "PAYMENT_INTENT";
        }
        if (callbackType.startsWith("REFUND_")) {
            return "REFUND";
        }
        return "PAYOUT_REQUEST";
    }

    private static long amount(ProviderCallbackCommand command) {
        return command.amountMinor() == null ? 0 : command.amountMinor();
    }

    private static UUID requireId(UUID value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireOneOf(String value, Set<String> allowed, String field) {
        String required = requireText(value, field);
        if (!allowed.contains(required)) {
            throw new BadRequestException(field + " is invalid.");
        }
        return required;
    }

    public static class ProviderCallbackApplyException extends RuntimeException {
        public ProviderCallbackApplyException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public record CreateProviderConfigCommand(String providerKey, String displayName, String signingSecret, String callbackToken) {
    }

    public record ProviderCallbackCommand(String providerEventId, Long providerSequence, String callbackType, UUID businessReferenceId, Long amountMinor, Map<String, Object> metadata) {
    }
}
