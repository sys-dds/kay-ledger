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
class Kay004Kay005FinancialCoreIntegrationTest {

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
    void kay004RecoveryAndKay005FinancialCoreWorkEndToEnd() throws Exception {
        List<Map<String, Object>> concurrentActors = concurrentActorCreates(headers("same-actor-key"), actorBody("finance-concurrent", "Finance Concurrent"));
        assertThat(concurrentActors).hasSize(2);
        assertThat(concurrentActors.get(0).get("id")).isEqualTo(concurrentActors.get(1).get("id"));
        assertStatus("/api/actors", HttpMethod.POST, headers("same-actor-key"), actorBody("finance-concurrent-different", "Different"), HttpStatus.BAD_REQUEST);

        post("/api/workspaces", headers("finance-workspace"), Map.of("slug", "finance-alpha", "displayName", "Finance Alpha"));
        post("/api/workspaces", headers("finance-beta-workspace"), Map.of("slug", "finance-beta", "displayName", "Finance Beta"));
        Map<String, Object> owner = post("/api/actors", headers("finance-owner"), actorBody("finance-owner", "Finance Owner"));
        Map<String, Object> provider = post("/api/actors", headers("finance-provider"), actorBody("finance-provider", "Finance Provider"));
        Map<String, Object> customer = post("/api/actors", headers("finance-customer"), actorBody("finance-customer", "Finance Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("finance-beta-owner"), actorBody("finance-beta-owner", "Finance Beta Owner"));

        post("/api/memberships", headers("finance-owner-membership"), Map.of("workspaceSlug", "finance-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("finance-provider-membership", "finance-alpha", "finance-owner"), Map.of("workspaceSlug", "finance-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("finance-customer-membership", "finance-alpha", "finance-owner"), Map.of("workspaceSlug", "finance-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("finance-beta-owner-membership"), Map.of("workspaceSlug", "finance-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("finance-owner-auth", "finance-alpha", "finance-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("finance-provider-profile", "finance-alpha", "finance-owner"), Map.of("actorId", provider.get("id"), "displayName", "Finance Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("finance-customer-profile", "finance-alpha", "finance-owner"), Map.of("actorId", customer.get("id"), "displayName", "Finance Customer"));

        Map<String, Object> receivable = post("/api/finance/accounts", workspaceHeaders("acct-receivable", "finance-alpha", "finance-owner"), account("1100", "Platform Receivable", "ASSET", "PLATFORM_RECEIVABLE"));
        Map<String, Object> sellerPayable = post("/api/finance/accounts", workspaceHeaders("acct-seller", "finance-alpha", "finance-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        Map<String, Object> feeRevenue = post("/api/finance/accounts", workspaceHeaders("acct-fee", "finance-alpha", "finance-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        Map<String, Object> receivableReplay = post("/api/finance/accounts", workspaceHeaders("acct-receivable", "finance-alpha", "finance-owner"), account("1100", "Platform Receivable", "ASSET", "PLATFORM_RECEIVABLE"));
        assertThat(receivableReplay.get("id")).isEqualTo(receivable.get("id"));

        Map<String, Object> offering = post("/api/offerings", workspaceHeaders("finance-offering", "finance-alpha", "finance-owner"), scheduledOffering(providerProfile.get("id")));
        String offeringId = offeringId(offering);
        post("/api/offerings/" + offeringId + "/publish", workspaceHeaders("finance-offering-publish", "finance-alpha", "finance-owner"), Map.of());

        Map<String, Object> feeRule = post("/api/finance/fee-rules", workspaceHeaders("fee-rule", "finance-alpha", "finance-owner"), Map.of(
                "offeringId", offeringId,
                "ruleType", "BASIS_POINTS",
                "basisPoints", 1000,
                "currencyCode", "USD"));
        assertThat(feeRule.get("basisPoints")).isEqualTo(1000);

        Map<String, Object> booking = post("/api/bookings", workspaceHeaders("finance-booking", "finance-alpha", "finance-customer"),
                scheduledBooking(offeringId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z"));
        Map<?, ?> bookingCore = (Map<?, ?>) booking.get("booking");
        assertThat(bookingCore.get("currencyCode")).isEqualTo("USD");
        assertThat(bookingCore.get("grossAmountMinor")).isEqualTo(10000);
        assertThat(bookingCore.get("feeAmountMinor")).isEqualTo(1000);
        assertThat(bookingCore.get("netAmountMinor")).isEqualTo(9000);

        String bookingId = (String) bookingCore.get("id");
        Map<String, Object> journal = post("/api/finance/journal-entries", workspaceHeaders("journal-booking", "finance-alpha", "finance-owner"), Map.of(
                "referenceType", "BOOKING",
                "referenceId", bookingId,
                "offeringId", offeringId,
                "bookingId", bookingId,
                "description", "Commercial booking snapshot only",
                "postings", List.of(
                        posting(receivable.get("id"), "DEBIT", 10000),
                        posting(sellerPayable.get("id"), "CREDIT", 9000),
                        posting(feeRevenue.get("id"), "CREDIT", 1000))));
        assertThat(((List<?>) journal.get("postings"))).hasSize(3);
        Map<String, Object> journalReplay = post("/api/finance/journal-entries", workspaceHeaders("journal-booking", "finance-alpha", "finance-owner"), Map.of(
                "referenceType", "BOOKING",
                "referenceId", bookingId,
                "offeringId", offeringId,
                "bookingId", bookingId,
                "description", "Commercial booking snapshot only",
                "postings", List.of(
                        posting(receivable.get("id"), "DEBIT", 10000),
                        posting(sellerPayable.get("id"), "CREDIT", 9000),
                        posting(feeRevenue.get("id"), "CREDIT", 1000))));
        assertThat(((Map<?, ?>) journalReplay.get("journalEntry")).get("id")).isEqualTo(((Map<?, ?>) journal.get("journalEntry")).get("id"));

        assertStatus("/api/finance/journal-entries", HttpMethod.POST, workspaceHeaders("journal-unbalanced", "finance-alpha", "finance-owner"), Map.of(
                "referenceType", "MANUAL",
                "description", "Bad entry",
                "postings", List.of(
                        posting(receivable.get("id"), "DEBIT", 10000),
                        posting(feeRevenue.get("id"), "CREDIT", 5000))), HttpStatus.BAD_REQUEST);

        Map<String, Object> balance = getMap("/api/finance/accounts/" + receivable.get("id") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(balance.get("debitAmountMinor")).isEqualTo(10000);
        assertThat(balance.get("creditAmountMinor")).isEqualTo(0);
        assertThat(balance.get("signedBalanceMinor")).isEqualTo(10000);

        List<?> referenceEntries = getList("/api/finance/journal-entries/by-reference?referenceType=BOOKING&referenceId=" + bookingId, ownerHeaders, HttpStatus.OK);
        assertThat(referenceEntries).hasSize(1);
        List<?> betaEntries = getList("/api/finance/journal-entries/by-reference?referenceType=BOOKING&referenceId=" + bookingId, workspaceHeaders("unused", "finance-beta", "finance-beta-owner"), HttpStatus.OK);
        assertThat(betaEntries).isEmpty();
    }

    private List<Map<String, Object>> concurrentActorCreates(HttpHeaders headers, Map<String, Object> body) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<Map<String, Object>> task = () -> post("/api/actors", headers, body);
            List<Future<Map<String, Object>>> futures = executor.invokeAll(List.of(task, task));
            List<Map<String, Object>> responses = new ArrayList<>();
            for (Future<Map<String, Object>> future : futures) {
                responses.add(future.get());
            }
            return responses;
        } finally {
            executor.shutdownNow();
        }
    }

    private Map<String, Object> actorBody(String actorKey, String displayName) {
        return Map.of("actorKey", actorKey, "displayName", displayName);
    }

    private Map<String, Object> account(String code, String name, String type, String purpose) {
        return Map.of("accountCode", code, "accountName", name, "accountType", type, "accountPurpose", purpose, "currencyCode", "USD");
    }

    private Map<String, Object> posting(Object accountId, String side, long amountMinor) {
        return Map.of("accountId", accountId, "entrySide", side, "amountMinor", amountMinor, "currencyCode", "USD");
    }

    private Map<String, Object> scheduledOffering(Object providerProfileId) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", "Finance Session"),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "financial-snapshot")),
                Map.entry("pricingRules", List.of(Map.of("ruleType", "FIXED_PRICE", "currencyCode", "USD", "amountMinor", 10000, "sortOrder", 0))),
                Map.entry("availabilityWindows", List.of(Map.of("weekday", 1, "startLocalTime", "09:00:00", "endLocalTime", "17:00:00"))));
    }

    private Map<String, Object> scheduledBooking(String offeringId, Object customerProfileId, String start, String end) {
        return Map.of(
                "offeringId", offeringId,
                "customerProfileId", customerProfileId,
                "scheduledStartAt", Instant.parse(start).toString(),
                "scheduledEndAt", Instant.parse(end).toString(),
                "holdTtlSeconds", 900);
    }

    private String offeringId(Map<String, Object> offeringDetails) {
        return (String) ((Map<?, ?>) offeringDetails.get("offering")).get("id");
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return asMap(response.getBody());
    }

    private Map<String, Object> getMap(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.GET, withoutIdempotency(headers), null, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody() == null ? Map.of() : asMap(response.getBody());
    }

    private List<?> getList(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<List> response = exchange(path, HttpMethod.GET, withoutIdempotency(headers), null, List.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody();
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

    private HttpHeaders withoutIdempotency(HttpHeaders source) {
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(source);
        headers.remove("Idempotency-Key");
        return headers;
    }

    private <T> ResponseEntity<T> exchange(String path, HttpMethod method, HttpHeaders headers, Object body, Class<T> responseType) {
        return restTemplate.exchange("http://localhost:" + port + path, method, new HttpEntity<>(body, headers), responseType);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Map<?, ?> value) {
        return (Map<String, Object>) value;
    }
}
