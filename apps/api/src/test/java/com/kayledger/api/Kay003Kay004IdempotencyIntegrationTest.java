package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
class Kay003Kay004IdempotencyIntegrationTest {

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
    void kay003RecoveryAndKay004IdempotencyWorkEndToEnd() throws Exception {
        assertStatus("/api/actors", HttpMethod.POST, new HttpHeaders(), actorBody("missing-key", "Missing Key"), HttpStatus.BAD_REQUEST);

        Map<String, Object> workspace = post("/api/workspaces", headers("idem-workspace"), workspaceBody("idem-alpha", "Idem Alpha"));
        Map<String, Object> workspaceReplay = post("/api/workspaces", headers("idem-workspace"), workspaceBody("idem-alpha", "Idem Alpha"));
        assertThat(workspaceReplay.get("id")).isEqualTo(workspace.get("id"));

        Map<String, Object> owner = post("/api/actors", headers("idem-owner"), actorBody("idem-owner", "Idem Owner"));
        Map<String, Object> ownerReplay = post("/api/actors", headers("idem-owner"), actorBody("idem-owner", "Idem Owner"));
        assertThat(ownerReplay.get("id")).isEqualTo(owner.get("id"));
        assertStatus("/api/actors", HttpMethod.POST, headers("idem-owner"), actorBody("idem-owner-different", "Different"), HttpStatus.BAD_REQUEST);

        Map<String, Object> provider = post("/api/actors", headers("idem-provider"), actorBody("idem-provider", "Idem Provider"));
        Map<String, Object> customer = post("/api/actors", headers("idem-customer"), actorBody("idem-customer", "Idem Customer"));
        Map<String, Object> operator = post("/api/actors", headers("idem-operator"), actorBody("idem-operator", "Idem Operator"));

        post("/api/operator/bootstrap/operator-role", bootstrapHeaders("idem-bootstrap", "test-bootstrap-operator-key", "idem-operator"), Map.of());
        Map<String, Object> bootstrapReplay = post("/api/operator/bootstrap/operator-role", bootstrapHeaders("idem-bootstrap", "test-bootstrap-operator-key", "idem-operator"), Map.of());
        assertThat(bootstrapReplay.get("status")).isEqualTo("OK");

        post("/api/memberships", headers("idem-owner-membership"), Map.of(
                "workspaceSlug", "idem-alpha",
                "actorId", owner.get("id"),
                "role", "OWNER"));
        HttpHeaders ownerHeaders = workspaceHeaders("idem-owner-auth", "idem-alpha", "idem-owner");
        Map<String, Object> providerMembership = post("/api/memberships", ownerHeaders, Map.of(
                "workspaceSlug", "idem-alpha",
                "actorId", provider.get("id"),
                "role", "PROVIDER"));
        Map<String, Object> providerMembershipReplay = post("/api/memberships", ownerHeaders, Map.of(
                "workspaceSlug", "idem-alpha",
                "actorId", provider.get("id"),
                "role", "PROVIDER"));
        assertThat(providerMembershipReplay.get("id")).isEqualTo(providerMembership.get("id"));
        post("/api/memberships", workspaceHeaders("idem-customer-membership", "idem-alpha", "idem-owner"), Map.of(
                "workspaceSlug", "idem-alpha",
                "actorId", customer.get("id"),
                "role", "CUSTOMER"));

        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("idem-provider-profile", "idem-alpha", "idem-owner"), Map.of(
                "actorId", provider.get("id"),
                "displayName", "Idem Provider Profile"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("idem-customer-profile", "idem-alpha", "idem-owner"), Map.of(
                "actorId", customer.get("id"),
                "displayName", "Idem Customer Profile"));

        Map<String, Object> scheduled = post("/api/offerings", workspaceHeaders("idem-scheduled-offering", "idem-alpha", "idem-owner"),
                scheduledOffering(providerProfile.get("id"), "Idem Scheduled", 10));
        Map<String, Object> scheduledReplay = post("/api/offerings", workspaceHeaders("idem-scheduled-offering", "idem-alpha", "idem-owner"),
                scheduledOffering(providerProfile.get("id"), "Idem Scheduled", 10));
        String scheduledId = offeringId(scheduled);
        assertThat(offeringId(scheduledReplay)).isEqualTo(scheduledId);
        Map<String, Object> published = post("/api/offerings/" + scheduledId + "/publish", workspaceHeaders("idem-publish-scheduled", "idem-alpha", "idem-owner"), Map.of());
        Map<String, Object> publishedReplay = post("/api/offerings/" + scheduledId + "/publish", workspaceHeaders("idem-publish-scheduled", "idem-alpha", "idem-owner"), Map.of());
        assertThat(((Map<?, ?>) publishedReplay.get("offering")).get("status")).isEqualTo(((Map<?, ?>) published.get("offering")).get("status"));

        Map<String, Object> stale = post("/api/bookings", workspaceHeaders("idem-stale-booking", "idem-alpha", "idem-customer"),
                scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z", 0));
        assertThat(((Map<?, ?>) stale.get("booking")).get("status")).isEqualTo("HELD");
        Map<String, Object> afterStaleRelease = post("/api/bookings", workspaceHeaders("idem-after-stale", "idem-alpha", "idem-customer"),
                scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z", 900));
        assertThat(((Map<?, ?>) afterStaleRelease.get("booking")).get("status")).isEqualTo("HELD");

        Map<String, Object> replayedBooking = post("/api/bookings", workspaceHeaders("idem-replay-booking", "idem-alpha", "idem-customer"),
                scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T12:00:00Z", "2030-01-07T13:00:00Z", 900));
        Map<String, Object> replayedBookingAgain = post("/api/bookings", workspaceHeaders("idem-replay-booking", "idem-alpha", "idem-customer"),
                scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T12:00:00Z", "2030-01-07T13:00:00Z", 900));
        assertThat(((Map<?, ?>) replayedBookingAgain.get("booking")).get("id")).isEqualTo(((Map<?, ?>) replayedBooking.get("booking")).get("id"));
        assertThat(((Map<?, ?>) replayedBookingAgain.get("hold")).get("id")).isEqualTo(((Map<?, ?>) replayedBooking.get("hold")).get("id"));
        assertStatus("/api/bookings", HttpMethod.POST, workspaceHeaders("idem-replay-booking", "idem-alpha", "idem-customer"),
                scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T14:00:00Z", "2030-01-07T15:00:00Z", 900), HttpStatus.BAD_REQUEST);

        String cancelId = (String) ((Map<?, ?>) replayedBooking.get("booking")).get("id");
        Map<String, Object> cancelled = post("/api/bookings/" + cancelId + "/cancel", workspaceHeaders("idem-cancel", "idem-alpha", "idem-customer"), Map.of());
        Map<String, Object> cancelledReplay = post("/api/bookings/" + cancelId + "/cancel", workspaceHeaders("idem-cancel", "idem-alpha", "idem-customer"), Map.of());
        assertThat(((Map<?, ?>) cancelledReplay.get("booking")).get("status")).isEqualTo(((Map<?, ?>) cancelled.get("booking")).get("status"));

        Map<String, Object> quantity = post("/api/offerings", workspaceHeaders("idem-quantity-offering", "idem-alpha", "idem-owner"),
                quantityOffering(providerProfile.get("id"), "Idem Quantity", 1));
        String quantityId = offeringId(quantity);
        post("/api/offerings/" + quantityId + "/publish", workspaceHeaders("idem-publish-quantity", "idem-alpha", "idem-owner"), Map.of());
        List<HttpStatus> quantityRace = concurrentQuantityStatuses(workspaceHeaders("race-", "idem-alpha", "idem-customer"), quantityId, customerProfile.get("id"));
        assertThat(quantityRace).containsExactlyInAnyOrder(HttpStatus.OK, HttpStatus.BAD_REQUEST);

        Map<String, Object> expiryQuantity = post("/api/offerings", workspaceHeaders("idem-expiry-quantity-offering", "idem-alpha", "idem-owner"),
                quantityOffering(providerProfile.get("id"), "Idem Expiry Quantity", 1));
        String expiryQuantityId = offeringId(expiryQuantity);
        post("/api/offerings/" + expiryQuantityId + "/publish", workspaceHeaders("idem-publish-expiry-quantity", "idem-alpha", "idem-owner"), Map.of());
        Map<String, Object> expiring = post("/api/bookings", workspaceHeaders("idem-expiring", "idem-alpha", "idem-customer"),
                quantityBooking(expiryQuantityId, customerProfile.get("id"), 1, 0));
        String expiringId = (String) ((Map<?, ?>) expiring.get("booking")).get("id");
        Map<String, Object> expired = post("/api/bookings/" + expiringId + "/expire", workspaceHeaders("idem-expire", "idem-alpha", "idem-customer"), Map.of());
        Map<String, Object> expiredReplay = post("/api/bookings/" + expiringId + "/expire", workspaceHeaders("idem-expire", "idem-alpha", "idem-customer"), Map.of());
        assertThat(((Map<?, ?>) expiredReplay.get("booking")).get("status")).isEqualTo(((Map<?, ?>) expired.get("booking")).get("status"));

        Map<String, Object> archivable = post("/api/offerings", workspaceHeaders("idem-archive-offering", "idem-alpha", "idem-owner"),
                scheduledOffering(providerProfile.get("id"), "Idem Archive", 10));
        String archivableId = offeringId(archivable);
        Map<String, Object> archived = post("/api/offerings/" + archivableId + "/archive", workspaceHeaders("idem-archive", "idem-alpha", "idem-owner"), Map.of());
        Map<String, Object> archivedReplay = post("/api/offerings/" + archivableId + "/archive", workspaceHeaders("idem-archive", "idem-alpha", "idem-owner"), Map.of());
        assertThat(((Map<?, ?>) archivedReplay.get("offering")).get("status")).isEqualTo(((Map<?, ?>) archived.get("offering")).get("status"));
    }

    private List<HttpStatus> concurrentQuantityStatuses(HttpHeaders baseHeaders, String offeringId, Object customerProfileId) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<HttpStatus> first = () -> statusOf(exchange("/api/bookings", HttpMethod.POST, withIdempotency(baseHeaders, "race-a"),
                    quantityBooking(offeringId, customerProfileId, 1, 900), String.class));
            Callable<HttpStatus> second = () -> statusOf(exchange("/api/bookings", HttpMethod.POST, withIdempotency(baseHeaders, "race-b"),
                    quantityBooking(offeringId, customerProfileId, 1, 900), String.class));
            List<Future<HttpStatus>> futures = executor.invokeAll(List.of(first, second));
            List<HttpStatus> statuses = new ArrayList<>();
            for (Future<HttpStatus> future : futures) {
                statuses.add(future.get());
            }
            return statuses;
        } finally {
            executor.shutdownNow();
        }
    }

    private Map<String, Object> actorBody(String actorKey, String displayName) {
        return Map.of("actorKey", actorKey, "displayName", displayName);
    }

    private Map<String, Object> workspaceBody(String slug, String displayName) {
        return Map.of("slug", slug, "displayName", displayName);
    }

    private Map<String, Object> scheduledOffering(Object providerProfileId, String title, int amountMinor) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "idempotent-booking-holds")),
                Map.entry("pricingRules", List.of(Map.of(
                        "ruleType", "FIXED_PRICE",
                        "currencyCode", "USD",
                        "amountMinor", amountMinor,
                        "sortOrder", 0))),
                Map.entry("availabilityWindows", List.of(Map.of(
                        "weekday", 1,
                        "startLocalTime", "09:00:00",
                        "endLocalTime", "17:00:00"))));
    }

    private Map<String, Object> quantityOffering(Object providerProfileId, String title, int quantityAvailable) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "QUANTITY"),
                Map.entry("pricingMetadata", Map.of("display", "per-unit")),
                Map.entry("quantityAvailable", quantityAvailable),
                Map.entry("schedulingMetadata", Map.of("boundary", "idempotent-quantity-holds")),
                Map.entry("pricingRules", List.of(Map.of(
                        "ruleType", "PER_UNIT",
                        "currencyCode", "USD",
                        "amountMinor", 2500,
                        "unitName", "item",
                        "sortOrder", 0))));
    }

    private Map<String, Object> scheduledBooking(String offeringId, Object customerProfileId, String start, String end, int holdTtlSeconds) {
        return Map.of(
                "offeringId", offeringId,
                "customerProfileId", customerProfileId,
                "scheduledStartAt", Instant.parse(start).toString(),
                "scheduledEndAt", Instant.parse(end).toString(),
                "holdTtlSeconds", holdTtlSeconds);
    }

    private Map<String, Object> quantityBooking(String offeringId, Object customerProfileId, int quantity, int holdTtlSeconds) {
        return Map.of(
                "offeringId", offeringId,
                "customerProfileId", customerProfileId,
                "quantity", quantity,
                "holdTtlSeconds", holdTtlSeconds);
    }

    private String offeringId(Map<String, Object> offeringDetails) {
        return (String) ((Map<?, ?>) offeringDetails.get("offering")).get("id");
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return asMap(response.getBody());
    }

    private void assertStatus(String path, HttpMethod method, HttpHeaders headers, Object body, HttpStatus expectedStatus) {
        ResponseEntity<String> response = exchange(path, method, headers, body, String.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
    }

    private HttpHeaders headers(String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Idempotency-Key", idempotencyKey);
        return headers;
    }

    private HttpHeaders workspaceHeaders(String idempotencyKey, String workspaceSlug, String actorKey) {
        HttpHeaders headers = headers(idempotencyKey);
        headers.add("X-Workspace-Slug", workspaceSlug);
        headers.add("X-Actor-Key", actorKey);
        return headers;
    }

    private HttpHeaders bootstrapHeaders(String idempotencyKey, String bootstrapKey, String actorKey) {
        HttpHeaders headers = headers(idempotencyKey);
        headers.add("X-Bootstrap-Key", bootstrapKey);
        headers.add("X-Actor-Key", actorKey);
        return headers;
    }

    private HttpHeaders withIdempotency(HttpHeaders baseHeaders, String suffix) {
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(baseHeaders);
        headers.set("Idempotency-Key", baseHeaders.getFirst("Idempotency-Key") + suffix);
        return headers;
    }

    private HttpStatus statusOf(ResponseEntity<?> response) {
        return HttpStatus.valueOf(response.getStatusCode().value());
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
