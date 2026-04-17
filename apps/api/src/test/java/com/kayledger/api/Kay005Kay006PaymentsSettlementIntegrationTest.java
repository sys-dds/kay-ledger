package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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
class Kay005Kay006PaymentsSettlementIntegrationTest {

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
    void kay005RecoveryAndKay006PaymentsSettlementWorkEndToEnd() {
        post("/api/workspaces", headers("ws-alpha"), Map.of("slug", "payments-alpha", "displayName", "Payments Alpha"));
        post("/api/workspaces", headers("ws-beta"), Map.of("slug", "payments-beta", "displayName", "Payments Beta"));

        Map<String, Object> owner = post("/api/actors", headers("actor-owner"), actor("payments-owner", "Payments Owner"));
        Map<String, Object> provider = post("/api/actors", headers("actor-provider"), actor("payments-provider", "Payments Provider"));
        Map<String, Object> customer = post("/api/actors", headers("actor-customer"), actor("payments-customer", "Payments Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("actor-beta-owner"), actor("payments-beta-owner", "Payments Beta Owner"));

        post("/api/memberships", headers("membership-owner"), Map.of("workspaceSlug", "payments-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("membership-provider", "payments-alpha", "payments-owner"), Map.of("workspaceSlug", "payments-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("membership-customer", "payments-alpha", "payments-owner"), Map.of("workspaceSlug", "payments-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("membership-beta-owner"), Map.of("workspaceSlug", "payments-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("owner-read", "payments-alpha", "payments-owner");
        HttpHeaders betaOwnerHeaders = workspaceHeaders("beta-read", "payments-beta", "payments-beta-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("provider-profile", "payments-alpha", "payments-owner"), Map.of("actorId", provider.get("id"), "displayName", "Payments Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("customer-profile", "payments-alpha", "payments-owner"), Map.of("actorId", customer.get("id"), "displayName", "Payments Customer"));

        Map<String, Object> authorizedFunds = post("/api/finance/accounts", workspaceHeaders("acct-authorized", "payments-alpha", "payments-owner"), account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS", "USD"));
        Map<String, Object> platformClearing = post("/api/finance/accounts", workspaceHeaders("acct-clearing", "payments-alpha", "payments-owner"), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING", "USD"));
        Map<String, Object> capturedFunds = post("/api/finance/accounts", workspaceHeaders("acct-captured", "payments-alpha", "payments-owner"), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS", "USD"));
        post("/api/finance/accounts", workspaceHeaders("acct-seller", "payments-alpha", "payments-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE", "USD"));
        post("/api/finance/accounts", workspaceHeaders("acct-fee", "payments-alpha", "payments-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE", "USD"));
        Map<String, Object> suspense = post("/api/finance/accounts", workspaceHeaders("acct-suspense", "payments-alpha", "payments-owner"), account("9999", "Suspense", "ASSET", "SUSPENSE", "USD"));
        Map<String, Object> betaAccount = post("/api/finance/accounts", workspaceHeaders("acct-beta", "payments-beta", "payments-beta-owner"), account("1110", "Beta Authorized Funds", "ASSET", "AUTHORIZED_FUNDS", "USD"));

        Map<String, Object> zeroBalance = getMap("/api/finance/accounts/" + suspense.get("id") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(zeroBalance.get("signedBalanceMinor")).isEqualTo(0);

        Map<String, Object> offering = post("/api/offerings", workspaceHeaders("offering", "payments-alpha", "payments-owner"), scheduledOffering(providerProfile.get("id"), "Payments Session", "USD"));
        String offeringId = offeringId(offering);
        post("/api/offerings/" + offeringId + "/publish", workspaceHeaders("offering-publish", "payments-alpha", "payments-owner"), Map.of());
        post("/api/finance/fee-rules", workspaceHeaders("fee-rule", "payments-alpha", "payments-owner"), Map.of(
                "offeringId", offeringId,
                "ruleType", "BASIS_POINTS",
                "basisPoints", 1000,
                "currencyCode", "USD"));

        Map<String, Object> brokenOffering = post("/api/offerings", workspaceHeaders("broken-offering", "payments-alpha", "payments-owner"), scheduledOffering(providerProfile.get("id"), "Broken Fee Session", "USD"));
        String brokenOfferingId = offeringId(brokenOffering);
        post("/api/offerings/" + brokenOfferingId + "/publish", workspaceHeaders("broken-offering-publish", "payments-alpha", "payments-owner"), Map.of());
        post("/api/finance/fee-rules", workspaceHeaders("broken-fee", "payments-alpha", "payments-owner"), Map.of(
                "offeringId", brokenOfferingId,
                "ruleType", "FLAT",
                "flatAmountMinor", 100,
                "currencyCode", "EUR"));
        assertStatus("/api/bookings", HttpMethod.POST, workspaceHeaders("broken-booking", "payments-alpha", "payments-customer"),
                scheduledBooking(brokenOfferingId, customerProfile.get("id"), "2030-01-07T12:00:00Z", "2030-01-07T13:00:00Z"), HttpStatus.BAD_REQUEST);

        Map<String, Object> booking = post("/api/bookings", workspaceHeaders("booking", "payments-alpha", "payments-customer"),
                scheduledBooking(offeringId, customerProfile.get("id"), "2030-01-07T10:00:00Z", "2030-01-07T11:00:00Z"));
        Map<?, ?> bookingCore = (Map<?, ?>) booking.get("booking");
        assertThat(bookingCore.get("grossAmountMinor")).isEqualTo(12000);
        assertThat(bookingCore.get("feeAmountMinor")).isEqualTo(1200);
        assertThat(bookingCore.get("netAmountMinor")).isEqualTo(10800);
        String bookingId = (String) bookingCore.get("id");

        assertStatus("/api/finance/journal-entries", HttpMethod.POST, workspaceHeaders("cross-tenant-journal", "payments-alpha", "payments-owner"), Map.of(
                "referenceType", "MANUAL",
                "description", "Bad cross tenant posting",
                "postings", List.of(
                        posting(authorizedFunds.get("id"), "DEBIT", 100),
                        posting(betaAccount.get("id"), "CREDIT", 100))), HttpStatus.BAD_REQUEST);

        Map<String, Object> manualJournal = post("/api/finance/journal-entries", workspaceHeaders("manual-journal", "payments-alpha", "payments-owner"), Map.of(
                "referenceType", "MANUAL",
                "description", "Immutable posted entry",
                "postings", List.of(
                        posting(authorizedFunds.get("id"), "DEBIT", 500),
                        posting(platformClearing.get("id"), "CREDIT", 500))));
        assertThat(((List<?>) manualJournal.get("postings"))).hasSize(2);

        Map<String, Object> intent = post("/api/payments/intents", workspaceHeaders("payment-intent", "payments-alpha", "payments-owner"), Map.of(
                "bookingId", bookingId,
                "externalReference", "internal-intent-1"));
        Map<?, ?> payment = (Map<?, ?>) intent.get("paymentIntent");
        assertThat(payment.get("grossAmountMinor")).isEqualTo(12000);
        assertThat(payment.get("feeAmountMinor")).isEqualTo(1200);
        assertThat(payment.get("netAmountMinor")).isEqualTo(10800);
        String paymentIntentId = (String) payment.get("id");

        Map<String, Object> intentReplay = post("/api/payments/intents", workspaceHeaders("payment-intent", "payments-alpha", "payments-owner"), Map.of(
                "bookingId", bookingId,
                "externalReference", "internal-intent-1"));
        assertThat(((Map<?, ?>) intentReplay.get("paymentIntent")).get("id")).isEqualTo(paymentIntentId);
        assertStatus("/api/payments/intents", HttpMethod.POST, workspaceHeaders("payment-intent", "payments-alpha", "payments-owner"), Map.of(
                "bookingId", bookingId,
                "externalReference", "different-payload"), HttpStatus.BAD_REQUEST);

        Map<String, Object> authorized = post("/api/payments/intents/" + paymentIntentId + "/authorize", workspaceHeaders("payment-authorize", "payments-alpha", "payments-owner"), Map.of("amountMinor", 12000));
        assertThat(((Map<?, ?>) authorized.get("paymentIntent")).get("status")).isEqualTo("AUTHORIZED");
        Map<String, Object> captured = post("/api/payments/intents/" + paymentIntentId + "/capture", workspaceHeaders("payment-capture", "payments-alpha", "payments-owner"), Map.of("amountMinor", 12000));
        assertThat(((Map<?, ?>) captured.get("paymentIntent")).get("status")).isEqualTo("CAPTURED");
        Map<String, Object> settled = post("/api/payments/intents/" + paymentIntentId + "/settle", workspaceHeaders("payment-settle", "payments-alpha", "payments-owner"), Map.of("amountMinor", 12000));
        assertThat(((Map<?, ?>) settled.get("paymentIntent")).get("status")).isEqualTo("SETTLED");

        List<?> paymentJournals = getList("/api/finance/journal-entries/by-reference?referenceType=PAYMENT&referenceId=" + paymentIntentId, ownerHeaders, HttpStatus.OK);
        assertThat(paymentJournals).hasSize(3);
        paymentJournals.forEach(entry -> assertBalanced((Map<?, ?>) entry));

        List<?> payables = getList("/api/finance/payable-balances?providerProfileId=" + providerProfile.get("id"), ownerHeaders, HttpStatus.OK);
        assertThat(payables).hasSize(1);
        assertThat(((Map<?, ?>) payables.get(0)).get("payableAmountMinor")).isEqualTo(10800);

        Map<String, Object> capturedBalance = getMap("/api/finance/accounts/" + capturedFunds.get("id") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(capturedBalance.get("signedBalanceMinor")).isEqualTo(0);
        Map<String, Object> byBooking = getMap("/api/payments/intents/by-booking/" + bookingId, ownerHeaders, HttpStatus.OK);
        assertThat(((Map<?, ?>) byBooking.get("paymentIntent")).get("id")).isEqualTo(paymentIntentId);
        assertThat(getList("/api/payments/intents", betaOwnerHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/finance/journal-entries/by-reference?referenceType=PAYMENT&referenceId=" + paymentIntentId, betaOwnerHeaders, HttpStatus.OK)).isEmpty();
    }

    private void assertBalanced(Map<?, ?> journalDetails) {
        long debits = 0;
        long credits = 0;
        for (Object postingObject : (List<?>) journalDetails.get("postings")) {
            Map<?, ?> posting = (Map<?, ?>) postingObject;
            if ("DEBIT".equals(posting.get("entrySide"))) {
                debits += ((Number) posting.get("amountMinor")).longValue();
            } else {
                credits += ((Number) posting.get("amountMinor")).longValue();
            }
        }
        assertThat(debits).isEqualTo(credits);
    }

    private Map<String, Object> actor(String actorKey, String displayName) {
        return Map.of("actorKey", actorKey, "displayName", displayName);
    }

    private Map<String, Object> account(String code, String name, String type, String purpose, String currencyCode) {
        return Map.of("accountCode", code, "accountName", name, "accountType", type, "accountPurpose", purpose, "currencyCode", currencyCode);
    }

    private Map<String, Object> posting(Object accountId, String side, long amountMinor) {
        return Map.of("accountId", accountId, "entrySide", side, "amountMinor", amountMinor, "currencyCode", "USD");
    }

    private Map<String, Object> scheduledOffering(Object providerProfileId, String title, String currencyCode) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", title),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed-plus-unit")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "payments")),
                Map.entry("pricingRules", List.of(
                        Map.of("ruleType", "FIXED_PRICE", "currencyCode", currencyCode, "amountMinor", 10000, "sortOrder", 0),
                        Map.of("ruleType", "PER_UNIT", "currencyCode", currencyCode, "amountMinor", 2000, "unitName", "attendee", "sortOrder", 1))),
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
