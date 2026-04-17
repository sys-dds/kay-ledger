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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Kay001Kay002RecoveryIntegrationTest {

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

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final AtomicInteger idempotencySequence = new AtomicInteger();

    @Test
    void kay001RecoveryAndKay002CatalogWorkEndToEnd() {
        assertStatus("/api/actors", HttpMethod.POST, new HttpHeaders(), Map.of(
                "actorKey", "operator-candidate",
                "displayName", "Operator Candidate",
                "platformRoles", List.of("OPERATOR")), HttpStatus.BAD_REQUEST);
        Map<String, Object> operatorCandidate = post("/api/actors", Map.of(
                "actorKey", "operator-candidate",
                "displayName", "Operator Candidate"));
        assertStatus("/api/operator/workspaces", HttpMethod.GET, actorHeader("operator-candidate"), null, HttpStatus.FORBIDDEN);
        assertStatus("/api/operator/bootstrap/operator-role", HttpMethod.POST, bootstrapHeaders("wrong-key", "operator-candidate"), null, HttpStatus.FORBIDDEN);
        assertStatus("/api/operator/bootstrap/operator-role", HttpMethod.POST, bootstrapHeaders("test-bootstrap-operator-key", "operator-candidate"), null, HttpStatus.OK);

        Map<String, Object> alphaWorkspace = post("/api/workspaces", Map.of(
                "slug", "alpha-workspace",
                "displayName", "Alpha Workspace"));
        Map<String, Object> betaWorkspace = post("/api/workspaces", Map.of(
                "slug", "beta-workspace",
                "displayName", "Beta Workspace"));
        List<?> operatorWorkspaces = getList("/api/operator/workspaces", actorHeader("operator-candidate"), HttpStatus.OK);
        assertThat(operatorWorkspaces).hasSize(2);
        assertThat(operatorCandidate.get("actorKey")).isEqualTo("operator-candidate");

        Map<String, Object> alphaOwner = post("/api/actors", Map.of("actorKey", "alpha-owner", "displayName", "Alpha Owner"));
        Map<String, Object> alphaProvider = post("/api/actors", Map.of("actorKey", "alpha-provider", "displayName", "Alpha Provider"));
        Map<String, Object> alphaProviderTwo = post("/api/actors", Map.of("actorKey", "alpha-provider-two", "displayName", "Alpha Provider Two"));
        Map<String, Object> alphaCustomer = post("/api/actors", Map.of("actorKey", "alpha-customer", "displayName", "Alpha Customer"));
        Map<String, Object> limitedAdmin = post("/api/actors", Map.of("actorKey", "limited-admin", "displayName", "Limited Admin"));
        Map<String, Object> betaOwner = post("/api/actors", Map.of("actorKey", "beta-owner", "displayName", "Beta Owner"));
        Map<String, Object> disabledActor = post("/api/actors", Map.of("actorKey", "disabled-actor", "displayName", "Disabled Actor"));

        post("/api/memberships", Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaOwner.get("id"),
                "role", "OWNER"));
        HttpHeaders alphaOwnerHeaders = workspaceHeaders("alpha-workspace", "alpha-owner");
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaProvider.get("id"),
                "role", "PROVIDER"));
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", alphaProviderTwo.get("id"),
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
        post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaWorkspace.get("slug"),
                "actorId", disabledActor.get("id"),
                "role", "ADMIN"));

        post("/api/memberships", Map.of(
                "workspaceSlug", betaWorkspace.get("slug"),
                "actorId", betaOwner.get("id"),
                "role", "OWNER"));

        Map<String, Object> providerProfile = post("/api/provider-profiles", alphaOwnerHeaders, Map.of(
                "actorId", alphaProvider.get("id"),
                "displayName", "Alpha Provider Profile"));
        Map<String, Object> providerTwoProfile = post("/api/provider-profiles", alphaOwnerHeaders, Map.of(
                "actorId", alphaProviderTwo.get("id"),
                "displayName", "Alpha Provider Two Profile"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", alphaOwnerHeaders, Map.of(
                "actorId", alphaCustomer.get("id"),
                "displayName", "Alpha Customer Profile"));
        assertThat(providerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));
        assertThat(customerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));

        assertStatus("/api/provider-profiles", HttpMethod.POST, alphaOwnerHeaders, Map.of(
                "actorId", alphaCustomer.get("id"),
                "displayName", "Invalid Provider Profile"), HttpStatus.FORBIDDEN);
        assertStatus("/api/customer-profiles", HttpMethod.POST, alphaOwnerHeaders, Map.of(
                "actorId", alphaProvider.get("id"),
                "displayName", "Invalid Customer Profile"), HttpStatus.FORBIDDEN);

        Map<String, Object> context = getMap("/api/access/context", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(context.get("workspaceRole")).isEqualTo("OWNER");
        assertThat(context.get("workspaceScopes")).asList().contains("CATALOG_WRITE", "CATALOG_PUBLISH");
        assertStatus("/api/access/context", HttpMethod.GET, new HttpHeaders(), null, HttpStatus.BAD_REQUEST);
        assertStatus("/api/access/context", HttpMethod.GET, workspaceHeaders("alpha-workspace", "unknown-actor"), null, HttpStatus.NOT_FOUND);
        assertStatus("/api/access/context", HttpMethod.GET, workspaceHeaders("alpha-workspace", "beta-owner"), null, HttpStatus.FORBIDDEN);

        jdbcTemplate.update("UPDATE actors SET status = 'DISABLED' WHERE actor_key = ?", "disabled-actor");
        assertStatus("/api/access/context", HttpMethod.GET, workspaceHeaders("alpha-workspace", "disabled-actor"), null, HttpStatus.NOT_FOUND);

        assertStatus("/api/actors", HttpMethod.GET, workspaceHeaders("alpha-workspace", "limited-admin"), null, HttpStatus.FORBIDDEN);
        List<?> betaProviders = getList("/api/provider-profiles", workspaceHeaders("beta-workspace", "beta-owner"), HttpStatus.OK);
        assertThat(betaProviders).isEmpty();

        Map<String, Object> offering = post("/api/offerings", alphaOwnerHeaders, offeringBody(providerProfile.get("id"), "Foundation Planning Session"));
        Map<?, ?> offeringCore = (Map<?, ?>) offering.get("offering");
        assertThat(offeringCore.get("status")).isEqualTo("DRAFT");
        assertThat(offeringCore.get("offerType")).isEqualTo("SCHEDULED_TIME");
        assertThat((List<?>) offering.get("pricingRules")).hasSize(1);
        assertThat((List<?>) offering.get("availabilityWindows")).hasSize(1);

        List<?> offerings = getList("/api/offerings", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(offerings).hasSize(1);

        Map<String, Object> published = post("/api/offerings/" + offeringCore.get("id") + "/publish", alphaOwnerHeaders, Map.of());
        assertThat(((Map<?, ?>) published.get("offering")).get("status")).isEqualTo("PUBLISHED");
        Map<String, Object> archived = post("/api/offerings/" + offeringCore.get("id") + "/archive", alphaOwnerHeaders, Map.of());
        assertThat(((Map<?, ?>) archived.get("offering")).get("status")).isEqualTo("ARCHIVED");

        Map<String, Object> providerTwoOffering = post("/api/offerings", alphaOwnerHeaders, offeringBody(providerTwoProfile.get("id"), "Provider Two Session"));
        String providerTwoOfferingId = (String) ((Map<?, ?>) providerTwoOffering.get("offering")).get("id");
        assertStatus("/api/offerings/" + providerTwoOfferingId + "/publish", HttpMethod.POST, workspaceHeaders("alpha-workspace", "alpha-provider"), Map.of(), HttpStatus.FORBIDDEN);
        assertStatus("/api/offerings", HttpMethod.POST, workspaceHeaders("alpha-workspace", "alpha-customer"), offeringBody(providerProfile.get("id"), "Customer Bad Offer"), HttpStatus.FORBIDDEN);
    }

    private Map<String, Object> offeringBody(Object providerProfileId, String title) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 120),
                Map.entry("maxNoticeDays", 45),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "availability-windows-only")),
                Map.entry("pricingRules", List.of(Map.of(
                        "ruleType", "FIXED_PRICE",
                        "currencyCode", "USD",
                        "amountMinor", 12500,
                        "sortOrder", 0))),
                Map.entry("availabilityWindows", List.of(Map.of(
                        "weekday", 1,
                        "startLocalTime", "09:00:00",
                        "endLocalTime", "17:00:00"))));
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
        return response.getBody() == null ? Map.of() : asMap(response.getBody());
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

    private HttpHeaders workspaceHeaders(String workspaceSlug, String actorKey) {
        HttpHeaders headers = actorHeader(actorKey);
        headers.add("X-Workspace-Slug", workspaceSlug);
        return headers;
    }

    private HttpHeaders actorHeader(String actorKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Actor-Key", actorKey);
        return headers;
    }

    private HttpHeaders bootstrapHeaders(String bootstrapKey, String actorKey) {
        HttpHeaders headers = actorHeader(actorKey);
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
        headers.add("Idempotency-Key", "kay001-kay002-" + path.replaceAll("[^A-Za-z0-9]", "-") + "-" + idempotencySequence.incrementAndGet());
        return headers;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }
}
