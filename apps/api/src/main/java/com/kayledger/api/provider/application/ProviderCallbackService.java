package com.kayledger.api.provider.application;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.payment.store.PaymentStore;
import com.kayledger.api.provider.model.ProviderCallback;
import com.kayledger.api.provider.model.ProviderConfig;
import com.kayledger.api.provider.store.ProviderStore;
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

    private final ProviderStore providerStore;
    private final PaymentStore paymentStore;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public ProviderCallbackService(ProviderStore providerStore, PaymentStore paymentStore, AccessPolicy accessPolicy, ObjectMapper objectMapper) {
        this.providerStore = providerStore;
        this.paymentStore = paymentStore;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ProviderConfig createConfig(AccessContext context, CreateProviderConfigCommand command) {
        requireProviderAdmin(context);
        return providerStore.createConfig(
                context.workspaceId(),
                requireText(command.providerKey(), "providerKey"),
                requireText(command.displayName(), "displayName"),
                requireText(command.signingSecret(), "signingSecret"));
    }

    public List<ProviderCallback> listCallbacks(AccessContext context) {
        requireProviderRead(context);
        return providerStore.listCallbacks(context.workspaceId());
    }

    @Transactional
    public ProviderCallback ingest(UUID workspaceId, String providerKey, String signatureHeader, ProviderCallbackCommand command) {
        ProviderConfig config = providerStore.findConfig(workspaceId, requireText(providerKey, "providerKey"))
                .orElseThrow(() -> new NotFoundException("Provider config was not found."));
        String payload = payloadJson(command);
        String expected = sign(config.signingSecret(), signingPayload(command));
        if (!expected.equals(requireText(signatureHeader, "X-Provider-Signature"))) {
            throw new ForbiddenException("Provider callback signature is invalid.");
        }
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
                payload,
                signatureHeader,
                true,
                dedupeKey);
        if (!"RECEIVED".equals(callback.processingStatus())) {
            return callback;
        }
        Long latest = providerStore.latestAppliedSequence(workspaceId, referenceType, referenceId);
        if (latest != null && callback.providerSequence() != null && callback.providerSequence() <= latest) {
            return providerStore.markIgnoredOutOfOrder(workspaceId, callback.id());
        }
        try {
            applyTruth(workspaceId, callbackType, referenceId, amount(command));
            return providerStore.markApplied(workspaceId, callback.id());
        } catch (RuntimeException exception) {
            return providerStore.markFailed(workspaceId, callback.id(), exception.getMessage());
        }
    }

    private void applyTruth(UUID workspaceId, String callbackType, UUID referenceId, long amountMinor) {
        switch (callbackType) {
            case "PAYMENT_AUTHORIZED" -> paymentStore.applyProviderPaymentStatus(workspaceId, referenceId, "AUTHORIZED", amountMinor);
            case "PAYMENT_CAPTURED" -> paymentStore.applyProviderPaymentStatus(workspaceId, referenceId, "CAPTURED", amountMinor);
            case "PAYMENT_SETTLED" -> paymentStore.applyProviderPaymentStatus(workspaceId, referenceId, "SETTLED", amountMinor);
            case "PAYMENT_FAILED" -> paymentStore.applyProviderPaymentStatus(workspaceId, referenceId, "FAILED", amountMinor);
            case "REFUND_SUCCEEDED" -> paymentStore.applyProviderRefundStatus(workspaceId, referenceId, "SUCCEEDED");
            case "REFUND_FAILED" -> paymentStore.applyProviderRefundStatus(workspaceId, referenceId, "FAILED");
            case "PAYOUT_SUCCEEDED" -> paymentStore.applyProviderPayoutStatus(workspaceId, referenceId, "SUCCEEDED", null);
            case "PAYOUT_FAILED" -> paymentStore.applyProviderPayoutStatus(workspaceId, referenceId, "FAILED", "provider reported failure");
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

    private String payloadJson(ProviderCallbackCommand command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("Provider callback payload could not be serialized.");
        }
    }

    private static String signingPayload(ProviderCallbackCommand command) {
        return command.providerEventId() + ":" + command.callbackType() + ":" + command.businessReferenceId() + ":" + command.amountMinor();
    }

    private static String sign(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new BadRequestException("Provider signature could not be verified.");
        }
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

    public record CreateProviderConfigCommand(String providerKey, String displayName, String signingSecret) {
    }

    public record ProviderCallbackCommand(String providerEventId, Long providerSequence, String callbackType, UUID businessReferenceId, Long amountMinor, Map<String, Object> metadata) {
    }
}
