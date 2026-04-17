package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Kay001DomainBackboneIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_test")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    private final AtomicInteger idempotencySequence = new AtomicInteger();

    @Test
    void kay001BackboneIsTenantSafeRoleScopedAndOperatorBounded() {
        Map<String, Object> alphaWorkspace = post("/api/workspaces", Map.of(
                "slug", "alpha-workspace",
                "displayName", "Alpha Workspace"));
        Map<String, Object> betaWorkspace = post("/api/workspaces", Map.of(
                "slug", "beta-workspace",
                "displayName", "Beta Workspace"));

        Map<String, Object> alphaOwner = post("/api/actors", Map.of(
                "actorKey", "alpha-owner",
                "displayName", "Alpha Owner"));
        Map<String, Object> alphaProvider = post("/api/actors", Map.of(
                "actorKey", "alpha-provider",
                "displayName", "Alpha Provider"));
        Map<String, Object> alphaCustomer = post("/api/actors", Map.of(
                "actorKey", "alpha-customer",
                "displayName", "Alpha Customer"));
        Map<String, Object> limitedAdmin = post("/api/actors", Map.of(
                "actorKey", "limited-admin",
                "displayName", "Limited Admin"));
        Map<String, Object> betaOwner = post("/api/actors", Map.of(
                "actorKey", "beta-owner",
                "displayName", "Beta Owner"));
        assertStatus("/api/actors", HttpMethod.POST, new HttpHeaders(), Map.of(
                "actorKey", "platform-operator",
                "displayName", "Platform Operator",
                "platformRoles", List.of("OPERATOR")), HttpStatus.BAD_REQUEST);
        Map<String, Object> operator = post("/api/actors", Map.of(
                "actorKey", "platform-operator",
                "displayName", "Platform Operator"));
        assertStatus(
                "/api/operator/bootstrap/operator-role",
                HttpMethod.POST,
                bootstrapHeaders("test-bootstrap-operator-key", "platform-operator"),
                null,
                HttpStatus.OK);

        post("/api/memberships", Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaOwner.get("id"),
                "role", "OWNER"));

        HttpHeaders alphaOwnerHeaders = headers("alpha-workspace", "alpha-owner");
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaProvider.get("id"),
                "role", "PROVIDER"));
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaCustomer.get("id"),
                "role", "CUSTOMER"));
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", limitedAdmin.get("id"),
                "role", "ADMIN",
                "scopes", List.of("WORKSPACE_READ", "ACCESS_CONTEXT_READ")));

        post("/api/memberships", Map.of(
                "workspaceSlug", betaWorkspace.get("slug"),
                "actorId", betaOwner.get("id"),
                "role", "OWNER"));

        Map<String, Object> providerProfile = post("/api/provider-profiles", alphaOwnerHeaders, Map.of(
                "actorId", alphaProvider.get("id"),
                "displayName", "Alpha Provider Profile"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", alphaOwnerHeaders, Map.of(
                "actorId", alphaCustomer.get("id"),
                "displayName", "Alpha Customer Profile"));
        assertThat(providerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));
        assertThat(customerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));

        Map<String, Object> context = getMap("/api/access/context", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(context.get("workspaceSlug")).isEqualTo("alpha-workspace");
        assertThat(context.get("actorKey")).isEqualTo("alpha-owner");
        assertThat(context.get("workspaceRole")).isEqualTo("OWNER");
        assertThat(context.get("workspaceScopes")).asList().contains("MEMBERSHIP_MANAGE", "PROFILE_MANAGE");

        assertStatus("/api/access/context", HttpMethod.GET, new HttpHeaders(), null, HttpStatus.BAD_REQUEST);
        assertStatus("/api/access/context", HttpMethod.GET, headers("alpha-workspace", "missing-actor"), null, HttpStatus.NOT_FOUND);
        assertStatus("/api/access/context", HttpMethod.GET, headers("alpha-workspace", "beta-owner"), null, HttpStatus.FORBIDDEN);

        HttpHeaders betaOwnerHeaders = headers("beta-workspace", "beta-owner");
        List<?> betaProviders = getList("/api/provider-profiles", betaOwnerHeaders, HttpStatus.OK);
        assertThat(betaProviders).isEmpty();

        assertStatus("/api/actors", HttpMethod.GET, headers("alpha-workspace", "limited-admin"), null, HttpStatus.FORBIDDEN);
        assertStatus("/api/operator/workspaces", HttpMethod.GET, singleActorHeader("alpha-owner"), null, HttpStatus.FORBIDDEN);

        List<?> operatorWorkspaces = getList("/api/operator/workspaces", singleActorHeader((String) operator.get("actorKey")), HttpStatus.OK);
        assertThat(operatorWorkspaces).hasSize(2);

        assertStatus(
                "/api/provider-profiles",
                HttpMethod.POST,
                alphaOwnerHeaders,
                Map.of(
                        "actorId", alphaCustomer.get("id"),
                        "displayName", "Invalid Provider Profile"),
                HttpStatus.FORBIDDEN);
        assertStatus(
                "/api/customer-profiles",
                HttpMethod.POST,
                alphaOwnerHeaders,
                Map.of(
                        "actorId", alphaProvider.get("id"),
                        "displayName", "Invalid Customer Profile"),
                HttpStatus.FORBIDDEN);

        List<?> alphaMemberships = getList("/api/memberships", alphaOwnerHeaders, HttpStatus.OK);
        List<?> alphaActors = getList("/api/actors", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(alphaMemberships).hasSize(4);
        assertThat(alphaActors).hasSize(4);
    }

    private Map<String, Object> post(String path, Map<String, Object> body) {
        return post(path, new HttpHeaders(), body);
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return asMap(response.getBody());
    }

    private Map<String, Object> getMap(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.GET, headers, null, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return asMap(response.getBody());
    }

    private List<?> getList(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<List> response = exchange(path, HttpMethod.GET, headers, null, List.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody();
    }

    private void assertStatus(String path, HttpMethod method, HttpHeaders headers, Object body, HttpStatus expectedStatus) {
        ResponseEntity<String> response = exchange(path, method, headers, body, String.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
    }

    private HttpHeaders headers(String workspaceSlug, String actorKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Workspace-Slug", workspaceSlug);
        headers.add("X-Actor-Key", actorKey);
        return headers;
    }

    private HttpHeaders singleActorHeader(String actorKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Actor-Key", actorKey);
        return headers;
    }

    private HttpHeaders bootstrapHeaders(String bootstrapKey, String actorKey) {
        HttpHeaders headers = singleActorHeader(actorKey);
        headers.add("X-Bootstrap-Key", bootstrapKey);
        return headers;
    }

    private <T> ResponseEntity<T> exchange(
            String path,
            HttpMethod method,
            HttpHeaders headers,
            Object body,
            Class<T> responseType) {
        HttpHeaders effectiveHeaders = method == HttpMethod.POST ? withIdempotency(headers, path) : headers;
        return restTemplate.exchange("http://localhost:" + port + path, method, new HttpEntity<>(body, effectiveHeaders), responseType);
    }

    private HttpHeaders withIdempotency(HttpHeaders source, String path) {
        if (source.containsKey("Idempotency-Key")) {
            return source;
        }
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(source);
        headers.add("Idempotency-Key", "kay001-" + path.replaceAll("[^A-Za-z0-9]", "-") + "-" + idempotencySequence.incrementAndGet());
        return headers;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }
}
