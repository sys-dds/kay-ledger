package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
class Kay002Kay003BookingContentionIntegrationTest {

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
    void kay002RecoveryAndKay003BookingsWorkEndToEnd() throws Exception {
        assertStatus("/api/actors", HttpMethod.POST, new HttpHeaders(), Map.of(
                "actorKey", "operator-candidate",
                "displayName", "Operator Candidate",
                "platformRoles", List.of("OPERATOR")), HttpStatus.BAD_REQUEST);
        Map<String, Object> operator = post("/api/actors", Map.of(
                "actorKey", "operator-candidate",
                "displayName", "Operator Candidate"));
        assertThat(operator.get("actorKey")).isEqualTo("operator-candidate");
        assertStatus("/api/operator/workspaces", HttpMethod.GET, actorHeader("operator-candidate"), null, HttpStatus.FORBIDDEN);
        assertStatus("/api/operator/bootstrap/operator-role", HttpMethod.POST, bootstrapHeaders("wrong-key", "operator-candidate"), null, HttpStatus.FORBIDDEN);
        assertStatus("/api/operator/bootstrap/operator-role", HttpMethod.POST, bootstrapHeaders("test-bootstrap-operator-key", "operator-candidate"), null, HttpStatus.OK);
        assertThat(getList("/api/operator/workspaces", actorHeader("operator-candidate"), HttpStatus.OK)).isEmpty();

        Map<String, Object> alphaWorkspace = post("/api/workspaces", Map.of("slug", "booking-alpha", "displayName", "Booking Alpha"));
        post("/api/workspaces", Map.of("slug", "booking-beta", "displayName", "Booking Beta"));
        Map<String, Object> owner = post("/api/actors", Map.of("actorKey", "booking-owner", "displayName", "Booking Owner"));
        Map<String, Object> provider = post("/api/actors", Map.of("actorKey", "booking-provider", "displayName", "Booking Provider"));
        Map<String, Object> providerTwo = post("/api/actors", Map.of("actorKey", "booking-provider-two", "displayName", "Booking Provider Two"));
        Map<String, Object> customer = post("/api/actors", Map.of("actorKey", "booking-customer", "displayName", "Booking Customer"));
        Map<String, Object> betaOwner = post("/api/actors", Map.of("actorKey", "booking-beta-owner", "displayName", "Booking Beta Owner"));

        post("/api/memberships", Map.of("workspaceSlug", "booking-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        HttpHeaders ownerHeaders = workspaceHeaders("booking-alpha", "booking-owner");
        post("/api/memberships", ownerHeaders, Map.of("workspaceSlug", "booking-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", ownerHeaders, Map.of("workspaceSlug", "booking-alpha", "actorId", providerTwo.get("id"), "role", "PROVIDER"));
        post("/api/memberships", ownerHeaders, Map.of("workspaceSlug", "booking-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", Map.of("workspaceSlug", "booking-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        Map<String, Object> providerProfile = post("/api/provider-profiles", ownerHeaders, Map.of(
                "actorId", provider.get("id"),
                "displayName", "Booking Provider Profile"));
        Map<String, Object> providerTwoProfile = post("/api/provider-profiles", ownerHeaders, Map.of(
                "actorId", providerTwo.get("id"),
                "displayName", "Booking Provider Two Profile"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", ownerHeaders, Map.of(
                "actorId", customer.get("id"),
                "displayName", "Booking Customer Profile"));
        HttpHeaders customerHeaders = workspaceHeaders("booking-alpha", "booking-customer");

        assertStatus("/api/offerings", HttpMethod.POST, ownerHeaders, scheduledOffering(providerProfile.get("id"), "Bad Scheduled", true, true, null), HttpStatus.BAD_REQUEST);
        assertStatus("/api/offerings", HttpMethod.POST, ownerHeaders, quantityOffering(providerProfile.get("id"), "Bad Quantity", 2, true, false), HttpStatus.BAD_REQUEST);
        assertStatus("/api/offerings", HttpMethod.POST, ownerHeaders, quantityOffering(providerProfile.get("id"), "Bad Quantity Windows", 2, false, true), HttpStatus.BAD_REQUEST);

        Map<String, Object> notReady = post("/api/offerings", ownerHeaders, scheduledOffering(providerProfile.get("id"), "Not Ready Session", false, false, null));
        String notReadyId = offeringId(notReady);
        assertStatus("/api/offerings/" + notReadyId + "/publish", HttpMethod.POST, ownerHeaders, Map.of(), HttpStatus.BAD_REQUEST);

        Map<String, Object> scheduled = post("/api/offerings", ownerHeaders, scheduledOffering(providerProfile.get("id"), "Published Session", true, false, null));
        String scheduledId = offeringId(scheduled);
        assertThat(((Map<?, ?>) scheduled.get("offering")).get("status")).isEqualTo("DRAFT");
        Map<String, Object> publishedScheduled = post("/api/offerings/" + scheduledId + "/publish", ownerHeaders, Map.of());
        assertThat(((Map<?, ?>) publishedScheduled.get("offering")).get("status")).isEqualTo("PUBLISHED");

        Map<String, Object> quantity = post("/api/offerings", ownerHeaders, quantityOffering(providerProfile.get("id"), "Published Quantity", 3, false, false));
        String quantityId = offeringId(quantity);
        post("/api/offerings/" + quantityId + "/publish", ownerHeaders, Map.of());

        Map<String, Object> providerTwoOffering = post("/api/offerings", ownerHeaders, scheduledOffering(providerTwoProfile.get("id"), "Provider Two Session", true, false, null));
        String providerTwoOfferingId = offeringId(providerTwoOffering);
        post("/api/offerings/" + providerTwoOfferingId + "/publish", ownerHeaders, Map.of());

        assertStatus("/api/bookings", HttpMethod.POST, customerHeaders, scheduledBooking(notReadyId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z", 900), HttpStatus.BAD_REQUEST);
        assertStatus("/api/bookings", HttpMethod.POST, customerHeaders, scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-08T10:00:00Z", "2030-01-08T11:00:00Z", 900), HttpStatus.BAD_REQUEST);
        assertStatus("/api/bookings", HttpMethod.POST, customerHeaders, scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T10:15:00Z", "2030-01-07T11:15:00Z", 900), HttpStatus.BAD_REQUEST);

        Map<String, Object> scheduledBooking = post("/api/bookings", customerHeaders, scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z", 900));
        assertThat(((Map<?, ?>) scheduledBooking.get("booking")).get("status")).isEqualTo("HELD");
        assertThat(((Map<?, ?>) scheduledBooking.get("hold")).get("status")).isEqualTo("HELD");

        Map<String, Object> quantityBooking = post("/api/bookings", customerHeaders, quantityBooking(quantityId, customerProfile.get("id"), 2, 900));
        assertThat(((Map<?, ?>) quantityBooking.get("booking")).get("quantityReserved")).isEqualTo(2);
        assertStatus("/api/bookings", HttpMethod.POST, customerHeaders, quantityBooking(quantityId, customerProfile.get("id"), 2, 900), HttpStatus.BAD_REQUEST);

        assertThat(getList("/api/bookings", workspaceHeaders("booking-beta", "booking-beta-owner"), HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/bookings", workspaceHeaders("booking-alpha", "booking-provider-two"), HttpStatus.OK)).isEmpty();
        assertStatus("/api/bookings", HttpMethod.POST, workspaceHeaders("booking-alpha", "booking-provider"), scheduledBooking(providerTwoOfferingId, customerProfile.get("id"), "2030-01-07T12:00:00Z", "2030-01-07T13:00:00Z", 900), HttpStatus.FORBIDDEN);

        String scheduledBookingId = (String) ((Map<?, ?>) scheduledBooking.get("booking")).get("id");
        Map<String, Object> cancelled = post("/api/bookings/" + scheduledBookingId + "/cancel", customerHeaders, Map.of());
        assertThat(((Map<?, ?>) cancelled.get("booking")).get("status")).isEqualTo("CANCELLED");

        Map<String, Object> expiring = post("/api/bookings", customerHeaders, quantityBooking(quantityId, customerProfile.get("id"), 1, 0));
        String expiringId = (String) ((Map<?, ?>) expiring.get("booking")).get("id");
        Map<String, Object> expired = post("/api/bookings/" + expiringId + "/expire", customerHeaders, Map.of());
        assertThat(((Map<?, ?>) expired.get("booking")).get("status")).isEqualTo("EXPIRED");

        Map<String, Object> staleScheduled = post("/api/bookings", customerHeaders, scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T13:00:00Z", "2030-01-07T14:00:00Z", 0));
        assertThat(((Map<?, ?>) staleScheduled.get("booking")).get("status")).isEqualTo("HELD");
        Map<String, Object> afterStaleRelease = post("/api/bookings", customerHeaders, scheduledBooking(scheduledId, customerProfile.get("id"), "2030-01-07T13:00:00Z", "2030-01-07T14:00:00Z", 900));
        assertThat(((Map<?, ?>) afterStaleRelease.get("booking")).get("status")).isEqualTo("HELD");

        List<HttpStatus> contentionStatuses = runConcurrentSlotRequests(customerHeaders, scheduledId, customerProfile.get("id"));
        assertThat(contentionStatuses).contains(HttpStatus.OK, HttpStatus.BAD_REQUEST);

        Map<String, Object> raceQuantity = post("/api/offerings", ownerHeaders, quantityOffering(providerProfile.get("id"), "Race Quantity", 1, false, false));
        String raceQuantityId = offeringId(raceQuantity);
        post("/api/offerings/" + raceQuantityId + "/publish", ownerHeaders, Map.of());
        List<HttpStatus> quantityContentionStatuses = runConcurrentQuantityRequests(customerHeaders, raceQuantityId, customerProfile.get("id"));
        assertThat(quantityContentionStatuses).containsExactlyInAnyOrder(HttpStatus.OK, HttpStatus.BAD_REQUEST);
    }

    private List<HttpStatus> runConcurrentSlotRequests(HttpHeaders headers, String offeringId, Object customerProfileId) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<HttpStatus> task = () -> statusOf(exchange(
                    "/api/bookings",
                    HttpMethod.POST,
                    headers,
                    scheduledBooking(offeringId, customerProfileId, "2030-01-07T12:00:00Z", "2030-01-07T13:00:00Z", 900),
                    String.class));
            List<Future<HttpStatus>> futures = executor.invokeAll(List.of(task, task));
            List<HttpStatus> statuses = new ArrayList<>();
            for (Future<HttpStatus> future : futures) {
                statuses.add(future.get());
            }
            return statuses;
        } finally {
            executor.shutdownNow();
        }
    }

    private List<HttpStatus> runConcurrentQuantityRequests(HttpHeaders headers, String offeringId, Object customerProfileId) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<HttpStatus> task = () -> statusOf(exchange(
                    "/api/bookings",
                    HttpMethod.POST,
                    headers,
                    quantityBooking(offeringId, customerProfileId, 1, 900),
                    String.class));
            List<Future<HttpStatus>> futures = executor.invokeAll(List.of(task, task));
            List<HttpStatus> statuses = new ArrayList<>();
            for (Future<HttpStatus> future : futures) {
                statuses.add(future.get());
            }
            return statuses;
        } finally {
            executor.shutdownNow();
        }
    }

    private HttpStatus statusOf(ResponseEntity<?> response) {
        return HttpStatus.valueOf(response.getStatusCode().value());
    }

    private Map<String, Object> scheduledOffering(Object providerProfileId, String title, boolean includeAvailability, boolean badQuantityField, Object ignored) {
        Map<String, Object> body = baseScheduledOffering(providerProfileId, title);
        if (!includeAvailability) {
            body.remove("availabilityWindows");
        }
        if (badQuantityField) {
            body.put("quantityAvailable", 2);
        }
        return body;
    }

    private Map<String, Object> baseScheduledOffering(Object providerProfileId, String title) {
        return mutableMap(Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "booking-holds-only")),
                Map.entry("pricingRules", List.of(Map.of(
                        "ruleType", "FIXED_PRICE",
                        "currencyCode", "USD",
                        "amountMinor", 10000,
                        "sortOrder", 0))),
                Map.entry("availabilityWindows", List.of(Map.of(
                        "weekday", 1,
                        "startLocalTime", "09:00:00",
                        "endLocalTime", "17:00:00")))));
    }

    private Map<String, Object> quantityOffering(Object providerProfileId, String title, int quantityAvailable, boolean badScheduledFields, boolean includeAvailability) {
        Map<String, Object> body = mutableMap(Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "QUANTITY"),
                Map.entry("pricingMetadata", Map.of("display", "per-unit")),
                Map.entry("quantityAvailable", quantityAvailable),
                Map.entry("schedulingMetadata", Map.of("boundary", "quantity-reservation-only")),
                Map.entry("pricingRules", List.of(Map.of(
                        "ruleType", "PER_UNIT",
                        "currencyCode", "USD",
                        "amountMinor", 2500,
                        "unitName", "item",
                        "sortOrder", 0)))));
        if (badScheduledFields) {
            body.put("durationMinutes", 60);
            body.put("slotIntervalMinutes", 30);
        }
        if (includeAvailability) {
            body.put("availabilityWindows", List.of(Map.of(
                    "weekday", 1,
                    "startLocalTime", "09:00:00",
                    "endLocalTime", "17:00:00")));
        }
        return body;
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

    private Map<String, Object> mutableMap(Map<String, Object> body) {
        return new java.util.LinkedHashMap<>(body);
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
        headers.add("Idempotency-Key", "kay002-kay003-" + path.replaceAll("[^A-Za-z0-9]", "-") + "-" + idempotencySequence.incrementAndGet());
        return headers;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }
}
