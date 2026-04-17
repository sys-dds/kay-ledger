package com.kayledger.api.shared.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.shared.api.BadRequestException;

@Service
public class IdempotencyService {

    private static final String COMPLETED = "COMPLETED";
    private static final String IN_PROGRESS = "IN_PROGRESS";

    private final IdempotencyStore idempotencyStore;
    private final ObjectMapper objectMapper;

    public IdempotencyService(IdempotencyStore idempotencyStore, ObjectMapper objectMapper) {
        this.idempotencyStore = idempotencyStore;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ResponseEntity<Object> run(
            String idempotencyKey,
            String scopeKind,
            UUID workspaceId,
            UUID actorId,
            String routeKey,
            Object requestFingerprint,
            Supplier<Object> mutation) {
        String requiredKey = requireIdempotencyKey(idempotencyKey);
        String requestHash = hash(routeKey, requestFingerprint);
        idempotencyStore.createIfAbsent(scopeKind, workspaceId, actorId, routeKey, requiredKey, requestHash);
        IdempotencyRecord record = idempotencyStore.findForUpdate(scopeKind, workspaceId, actorId, routeKey, requiredKey)
                .orElseThrow(() -> new BadRequestException("Idempotency record could not be resolved."));
        if (!record.requestHash().equals(requestHash)) {
            throw new BadRequestException("Idempotency-Key was already used with a different request payload.");
        }
        if (COMPLETED.equals(record.status())) {
            return ResponseEntity.status(record.responseStatusCode()).body(json(record.responseBody()));
        }
        if (!IN_PROGRESS.equals(record.status())) {
            throw new BadRequestException("Idempotency-Key is in an unsupported state.");
        }

        Object responseBody = mutation.get();
        String responseJson = toJson(responseBody == null ? java.util.Map.of() : responseBody);
        idempotencyStore.complete(record.id(), HttpStatus.OK.value(), responseJson);
        return ResponseEntity.ok(responseBody == null ? java.util.Map.of() : responseBody);
    }

    public static List<Object> fingerprint(Object... parts) {
        return Arrays.asList(parts);
    }

    private String requireIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new BadRequestException("Idempotency-Key header is required for this mutation.");
        }
        return idempotencyKey.trim();
    }

    private String hash(String routeKey, Object requestFingerprint) {
        return sha256(toJson(List.of(routeKey, requestFingerprint)));
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("Request payload could not be fingerprinted.");
        }
    }

    private JsonNode json(String responseBody) {
        try {
            return objectMapper.readTree(responseBody);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("Stored idempotency response could not be replayed.");
        }
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available.", exception);
        }
    }
}
