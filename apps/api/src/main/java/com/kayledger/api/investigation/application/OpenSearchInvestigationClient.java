package com.kayledger.api.investigation.application;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.investigation.model.InvestigationSearchHit;

@Component
public class OpenSearchInvestigationClient {

    private final InvestigationSearchProperties properties;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean indexEnsured = new AtomicBoolean(false);
    private final Object indexEnsureLock = new Object();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public OpenSearchInvestigationClient(InvestigationSearchProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public void ensureIndex() {
        if (indexEnsured.get()) {
            return;
        }
        synchronized (indexEnsureLock) {
            if (indexEnsured.get()) {
                return;
            }
            createIndexIfNeeded();
            indexEnsured.set(true);
        }
    }

    private void createIndexIfNeeded() {
        try {
            HttpRequest request = HttpRequest.newBuilder(indexUri())
                    .timeout(Duration.ofSeconds(10))
                    .PUT(HttpRequest.BodyPublishers.ofString(indexDefinition()))
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400 && !indexAlreadyExists(response)) {
                throw new IllegalStateException("OpenSearch index creation failed with status " + response.statusCode());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("OpenSearch index could not be ensured.", exception);
        }
    }

    public void index(InvestigationDocument document) {
        ensureIndex();
        try {
            String body = objectMapper.writeValueAsString(document);
            HttpRequest request = HttpRequest.newBuilder(indexUri("_doc/" + encode(document.documentId())))
                    .timeout(Duration.ofSeconds(10))
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IllegalStateException("OpenSearch index write failed with status " + response.statusCode());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Investigation document could not be indexed.", exception);
        }
    }

    private static boolean indexAlreadyExists(HttpResponse<String> response) {
        return response.statusCode() == 400 && response.body() != null && response.body().contains("resource_already_exists_exception");
    }

    private static String indexDefinition() {
        return """
                {
                  "settings": {
                    "index": {
                      "number_of_shards": 1,
                      "number_of_replicas": 0
                    },
                    "analysis": {
                      "normalizer": {
                        "exact_lowercase": {
                          "type": "custom",
                          "filter": ["lowercase"]
                        }
                      }
                    }
                  },
                  "mappings": {
                    "dynamic": false,
                    "properties": {
                      "documentId": {"type": "keyword"},
                      "workspaceId": {"type": "keyword"},
                      "documentType": {"type": "keyword"},
                      "referenceType": {"type": "keyword"},
                      "referenceId": {"type": "keyword"},
                      "providerProfileId": {"type": "keyword"},
                      "paymentIntentId": {"type": "keyword"},
                      "refundId": {"type": "keyword"},
                      "payoutRequestId": {"type": "keyword"},
                      "disputeId": {"type": "keyword"},
                      "subscriptionId": {"type": "keyword"},
                      "providerEventId": {"type": "keyword"},
                      "externalReference": {"type": "keyword"},
                      "businessReferenceType": {"type": "keyword"},
                      "businessReferenceId": {"type": "keyword"},
                      "status": {"type": "keyword"},
                      "currencyCode": {"type": "keyword"},
                      "amountMinor": {"type": "long"},
                      "occurredAt": {"type": "date"},
                      "data": {"type": "object", "enabled": false}
                    }
                  }
                }
                """;
    }

    public List<InvestigationSearchHit> search(Map<String, Object> query) {
        ensureIndex();
        try {
            HttpRequest request = HttpRequest.newBuilder(indexUri("_search"))
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(query)))
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IllegalStateException("OpenSearch search failed with status " + response.statusCode());
            }
            Map<String, Object> raw = objectMapper.readValue(response.body(), new TypeReference<>() {
            });
            Map<String, Object> hits = castMap(raw.get("hits"));
            List<Map<String, Object>> rawHits = castList(hits.get("hits"));
            List<InvestigationSearchHit> results = new ArrayList<>();
            for (Map<String, Object> hit : rawHits) {
                Map<String, Object> source = castMap(hit.get("_source"));
                results.add(new InvestigationSearchHit(
                        hit.get("_id").toString(),
                        string(source.get("documentType")),
                        string(source.get("referenceType")),
                        string(source.get("referenceId")),
                        string(source.get("status")),
                        source));
            }
            return results;
        } catch (Exception exception) {
            throw new IllegalStateException("Investigation search failed.", exception);
        }
    }

    private URI indexUri() {
        return indexUri("");
    }

    private URI indexUri(String suffix) {
        String base = properties.getEndpoint().replaceAll("/+$", "");
        String index = encode(properties.getInvestigationIndex());
        String path = suffix == null || suffix.isBlank() ? index : index + "/" + suffix;
        return URI.create(base + "/" + path);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> castList(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
