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
import com.kayledger.api.payment.application.PaymentService;
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
    private final PaymentService paymentService;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public ProviderCallbackService(ProviderStore providerStore, PaymentService paymentService, AccessPolicy accessPolicy, ObjectMapper objectMapper) {
        this.providerStore = providerStore;
        this.paymentService = paymentService;
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
        return ingest(config, signatureHeader, command);
    }

    @Transactional
    public ProviderCallback ingestExternal(String workspaceSlug, String providerKey, String signatureHeader, ProviderCallbackCommand command) {
        ProviderConfig config = providerStore.findConfigByWorkspaceSlug(requireText(workspaceSlug, "X-Workspace-Slug"), requireText(providerKey, "providerKey"))
                .orElseThrow(() -> new NotFoundException("Provider config was not found."));
        return ingest(config, signatureHeader, command);
    }

    private ProviderCallback ingest(ProviderConfig config, String signatureHeader, ProviderCallbackCommand command) {
        UUID workspaceId = config.workspaceId();
        String payload = payloadJson(command);
        String expected = sign(config.signingSecret(), payload);
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
            applyTruth(workspaceId, callbackType, referenceId, amount(command), command.providerEventId());
            return providerStore.markApplied(workspaceId, callback.id());
        } catch (RuntimeException exception) {
            return providerStore.markFailed(workspaceId, callback.id(), exception.getMessage());
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

    private String payloadJson(ProviderCallbackCommand command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("Provider callback payload could not be serialized.");
        }
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
