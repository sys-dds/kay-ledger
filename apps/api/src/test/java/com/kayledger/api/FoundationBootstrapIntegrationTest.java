package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

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
class FoundationBootstrapIntegrationTest {

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

    @Test
    void foundationSliceWorksEndToEndWithTenantSafeAccess() {
        Map<String, Object> alphaWorkspace = post("/api/workspaces", Map.of(
                "slug", "alpha-workspace",
                "displayName", "Alpha Workspace"));
        String alphaSlug = (String) alphaWorkspace.get("slug");
        assertThat(alphaSlug).isEqualTo("alpha-workspace");

        Map<String, Object> owner = post("/api/actors", Map.of(
                "actorKey", "alpha-owner",
                "displayName", "Alpha Owner"));
        Map<String, Object> provider = post("/api/actors", Map.of(
                "actorKey", "alpha-provider",
                "displayName", "Alpha Provider"));
        Map<String, Object> customer = post("/api/actors", Map.of(
                "actorKey", "alpha-customer",
                "displayName", "Alpha Customer"));

        Map<String, Object> ownerMembership = post("/api/memberships", Map.of(
                "workspaceSlug", alphaSlug,
                "actorId", owner.get("id"),
                "role", "OWNER"));
        assertThat(ownerMembership.get("role")).isEqualTo("OWNER");

        HttpHeaders alphaOwnerHeaders = headers(alphaSlug, "alpha-owner");
        Map<String, Object> providerMembership = post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaSlug,
                "actorId", provider.get("id"),
                "role", "PROVIDER"));
        Map<String, Object> customerMembership = post("/api/memberships", alphaOwnerHeaders, Map.of(
                "workspaceSlug", alphaSlug,
                "actorId", customer.get("id"),
                "role", "CUSTOMER"));
        assertThat(providerMembership.get("role")).isEqualTo("PROVIDER");
        assertThat(customerMembership.get("role")).isEqualTo("CUSTOMER");

        Map<String, Object> providerProfile = post("/api/provider-profiles", alphaOwnerHeaders, Map.of(
                "actorId", provider.get("id"),
                "displayName", "Alpha Provider Profile"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", alphaOwnerHeaders, Map.of(
                "actorId", customer.get("id"),
                "displayName", "Alpha Customer Profile"));
        assertThat(providerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));
        assertThat(customerProfile.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));

        Map<String, Object> offering = post("/api/offerings", alphaOwnerHeaders, Map.of(
                "providerProfileId", providerProfile.get("id"),
                "title", "Foundation Planning Session",
                "pricingMetadata", Map.of("currency", "USD", "amountMinor", 12500),
                "durationMinutes", 60,
                "schedulingMetadata", Map.of("boundary", "weekday-business-hours")));
        assertThat(offering.get("workspaceId")).isEqualTo(alphaWorkspace.get("id"));
        assertThat(offering.get("providerProfileId")).isEqualTo(providerProfile.get("id"));
        assertThat(offering.get("title")).isEqualTo("Foundation Planning Session");

        List<?> alphaOfferings = getList("/api/offerings", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(alphaOfferings).hasSize(1);
        assertThat((String) ((Map<?, ?>) alphaOfferings.getFirst()).get("title")).isEqualTo("Foundation Planning Session");

        List<?> alphaProviders = getList("/api/provider-profiles", alphaOwnerHeaders, HttpStatus.OK);
        List<?> alphaCustomers = getList("/api/customer-profiles", alphaOwnerHeaders, HttpStatus.OK);
        List<?> alphaMemberships = getList("/api/memberships", alphaOwnerHeaders, HttpStatus.OK);
        List<?> alphaActors = getList("/api/actors", alphaOwnerHeaders, HttpStatus.OK);
        List<?> alphaWorkspaces = getList("/api/workspaces", alphaOwnerHeaders, HttpStatus.OK);
        assertThat(alphaProviders).hasSize(1);
        assertThat(alphaCustomers).hasSize(1);
        assertThat(alphaMemberships).hasSize(3);
        assertThat(alphaActors).hasSize(3);
        assertThat(alphaWorkspaces).hasSize(1);

        Map<String, Object> betaWorkspace = post("/api/workspaces", Map.of(
                "slug", "beta-workspace",
                "displayName", "Beta Workspace"));
        Map<String, Object> betaOwner = post("/api/actors", Map.of(
                "actorKey", "beta-owner",
                "displayName", "Beta Owner"));
        post("/api/memberships", Map.of(
                "workspaceSlug", betaWorkspace.get("slug"),
                "actorId", betaOwner.get("id"),
                "role", "OWNER"));

        HttpHeaders betaOwnerHeaders = headers((String) betaWorkspace.get("slug"), "beta-owner");
        List<?> betaOfferings = getList("/api/offerings", betaOwnerHeaders, HttpStatus.OK);
        assertThat(betaOfferings).isEmpty();

        assertStatus("/api/offerings", HttpMethod.GET, headers(alphaSlug, "beta-owner"), null, HttpStatus.FORBIDDEN);

        ResponseEntity<Map> crossTenantCreate = exchange(
                "/api/offerings",
                HttpMethod.POST,
                betaOwnerHeaders,
                Map.of(
                        "providerProfileId", providerProfile.get("id"),
                        "title", "Wrong Tenant Offering",
                        "pricingMetadata", Map.of("currency", "USD"),
                        "durationMinutes", 30,
                        "schedulingMetadata", Map.of("boundary", "none")),
                Map.class);
        assertThat(crossTenantCreate.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private Map<String, Object> post(String path, Map<String, Object> body) {
        return post(path, new HttpHeaders(), body);
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
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

    private <T> ResponseEntity<T> exchange(
            String path,
            HttpMethod method,
            HttpHeaders headers,
            Object body,
            Class<T> responseType) {
        return restTemplate.exchange("http://localhost:" + port + path, method, new HttpEntity<>(body, headers), responseType);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }
}
