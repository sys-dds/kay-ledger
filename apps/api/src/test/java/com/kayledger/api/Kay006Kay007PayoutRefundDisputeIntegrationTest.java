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
class Kay006Kay007PayoutRefundDisputeIntegrationTest {

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
    void kay006RecoveryAndKay007PayoutRefundDisputeWorkEndToEnd() {
        post("/api/workspaces", headers("ws-kay007"), Map.of("slug", "kay007-alpha", "displayName", "KAY007 Alpha"));
        post("/api/workspaces", headers("ws-kay007-beta"), Map.of("slug", "kay007-beta", "displayName", "KAY007 Beta"));

        Map<String, Object> owner = post("/api/actors", headers("actor-owner-kay007"), actor("kay007-owner", "KAY007 Owner"));
        Map<String, Object> provider = post("/api/actors", headers("actor-provider-kay007"), actor("kay007-provider", "KAY007 Provider"));
        Map<String, Object> customer = post("/api/actors", headers("actor-customer-kay007"), actor("kay007-customer", "KAY007 Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("actor-beta-kay007"), actor("kay007-beta-owner", "KAY007 Beta Owner"));

        post("/api/memberships", headers("membership-owner-kay007"), Map.of("workspaceSlug", "kay007-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("membership-provider-kay007", "kay007-alpha", "kay007-owner"), Map.of("workspaceSlug", "kay007-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("membership-customer-kay007", "kay007-alpha", "kay007-owner"), Map.of("workspaceSlug", "kay007-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("membership-beta-kay007"), Map.of("workspaceSlug", "kay007-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("owner-read-kay007", "kay007-alpha", "kay007-owner");
        HttpHeaders betaHeaders = workspaceHeaders("beta-read-kay007", "kay007-beta", "kay007-beta-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("provider-profile-kay007", "kay007-alpha", "kay007-owner"), Map.of("actorId", provider.get("id"), "displayName", "KAY007 Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("customer-profile-kay007", "kay007-alpha", "kay007-owner"), Map.of("actorId", customer.get("id"), "displayName", "KAY007 Customer"));

        Map<String, Object> authorizedFunds = account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS");
        Map<String, Object> payoutClearing = account("1140", "Payout Clearing", "LIABILITY", "PAYOUT_CLEARING");
        post("/api/finance/accounts", workspaceHeaders("acct-authorized-kay007", "kay007-alpha", "kay007-owner"), authorizedFunds);
        post("/api/finance/accounts", workspaceHeaders("acct-platform-kay007", "kay007-alpha", "kay007-owner"), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("acct-captured-kay007", "kay007-alpha", "kay007-owner"), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-payout-kay007", "kay007-alpha", "kay007-owner"), payoutClearing);
        post("/api/finance/accounts", workspaceHeaders("acct-seller-kay007", "kay007-alpha", "kay007-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("acct-fee-kay007", "kay007-alpha", "kay007-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        post("/api/finance/accounts", workspaceHeaders("acct-refund-kay007", "kay007-alpha", "kay007-owner"), account("2200", "Refund Reserve", "LIABILITY", "REFUND_RESERVE"));
        post("/api/finance/accounts", workspaceHeaders("acct-refund-liability-kay007", "kay007-alpha", "kay007-owner"), account("2210", "Refund Liability", "LIABILITY", "REFUND_LIABILITY"));
        post("/api/finance/accounts", workspaceHeaders("acct-dispute-kay007", "kay007-alpha", "kay007-owner"), account("2300", "Dispute Reserve", "LIABILITY", "DISPUTE_RESERVE"));
        post("/api/finance/accounts", workspaceHeaders("acct-frozen-kay007", "kay007-alpha", "kay007-owner"), account("2400", "Frozen Payable", "LIABILITY", "FROZEN_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("acct-cash-kay007", "kay007-alpha", "kay007-owner"), account("1000", "Cash Placeholder", "ASSET", "CASH_PLACEHOLDER"));

        Map<String, Object> offering = post("/api/offerings", workspaceHeaders("offering-kay007", "kay007-alpha", "kay007-owner"), scheduledOffering(providerProfile.get("id")));
        String offeringId = offeringId(offering);
        post("/api/offerings/" + offeringId + "/publish", workspaceHeaders("publish-kay007", "kay007-alpha", "kay007-owner"), Map.of());
        post("/api/finance/fee-rules", workspaceHeaders("fee-kay007", "kay007-alpha", "kay007-owner"), Map.of(
                "offeringId", offeringId,
                "ruleType", "BASIS_POINTS",
                "basisPoints", 1000,
                "currencyCode", "USD"));

        String firstPaymentId = settledPayment(offeringId, customerProfile.get("id"), "2031-01-06T10:00:00Z", "2031-01-06T11:00:00Z", "first", ownerHeaders);
        String secondPaymentId = settledPayment(offeringId, customerProfile.get("id"), "2031-01-06T11:00:00Z", "2031-01-06T12:00:00Z", "second", ownerHeaders);
        assertThat(balanceSummary(ownerHeaders).get("payableAmountMinor")).isEqualTo(21600);

        String cancelPaymentId = paymentForHeldBooking(offeringId, customerProfile.get("id"), "2031-01-06T12:00:00Z", "2031-01-06T13:00:00Z", "cancel");
        post("/api/payments/intents/" + cancelPaymentId + "/authorize", workspaceHeaders("cancel-authorize-kay007", "kay007-alpha", "kay007-owner"), Map.of("amountMinor", 12000));
        post("/api/payments/intents/" + cancelPaymentId + "/cancel", workspaceHeaders("cancel-kay007", "kay007-alpha", "kay007-owner"), Map.of());
        List<?> cancelJournals = getList("/api/finance/journal-entries/by-reference?referenceType=PAYMENT&referenceId=" + cancelPaymentId, ownerHeaders, HttpStatus.OK);
        assertThat(cancelJournals).hasSize(2);
        cancelJournals.forEach(entry -> assertBalanced((Map<?, ?>) entry));
        Map<String, Object> authorizedBalance = getMap("/api/finance/accounts/" + accountIdByPurpose(ownerHeaders, "AUTHORIZED_FUNDS") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(authorizedBalance.get("signedBalanceMinor")).isEqualTo(0);
        Map<String, Object> capturedBalance = getMap("/api/finance/accounts/" + accountIdByPurpose(ownerHeaders, "CAPTURED_FUNDS") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(capturedBalance.get("signedBalanceMinor")).isEqualTo(0);

        Map<String, Object> payout = post("/api/payments/payouts", workspaceHeaders("payout-request-kay007", "kay007-alpha", "kay007-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "currencyCode", "USD",
                "amountMinor", 5000));
        String payoutId = (String) payout.get("id");
        Map<String, Object> payoutReplay = post("/api/payments/payouts", workspaceHeaders("payout-request-kay007", "kay007-alpha", "kay007-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "currencyCode", "USD",
                "amountMinor", 5000));
        assertThat(payoutReplay.get("id")).isEqualTo(payoutId);
        assertStatus("/api/payments/payouts", HttpMethod.POST, workspaceHeaders("payout-request-kay007", "kay007-alpha", "kay007-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "currencyCode", "USD",
                "amountMinor", 6000), HttpStatus.BAD_REQUEST);
        assertStatus("/api/payments/payouts", HttpMethod.POST, workspaceHeaders("payout-too-large-kay007", "kay007-alpha", "kay007-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "currencyCode", "USD",
                "amountMinor", 999999), HttpStatus.BAD_REQUEST);
        post("/api/payments/payouts/" + payoutId + "/fail", workspaceHeaders("payout-fail-kay007", "kay007-alpha", "kay007-owner"), Map.of("failureReason", "temporary bank outage"));
        post("/api/payments/payouts/" + payoutId + "/retry", workspaceHeaders("payout-retry-kay007", "kay007-alpha", "kay007-owner"), Map.of());
        post("/api/payments/payouts/" + payoutId + "/succeed", workspaceHeaders("payout-succeed-kay007", "kay007-alpha", "kay007-owner"), Map.of());
        assertThat((List<?>) getList("/api/payments/payouts/" + payoutId + "/attempts", ownerHeaders, HttpStatus.OK)).hasSize(3);

        Map<String, Object> fullRefund = post("/api/payments/refunds/full", workspaceHeaders("full-refund-kay007", "kay007-alpha", "kay007-owner"), Map.of("paymentIntentId", firstPaymentId));
        Map<String, Object> partialRefund = post("/api/payments/refunds/partial", workspaceHeaders("partial-refund-kay007", "kay007-alpha", "kay007-owner"), Map.of("paymentIntentId", secondPaymentId, "amountMinor", 1000));
        Map<String, Object> reversal = post("/api/payments/reversals", workspaceHeaders("reversal-kay007", "kay007-alpha", "kay007-owner"), Map.of("paymentIntentId", secondPaymentId, "amountMinor", 500));
        post("/api/payments/refunds/" + partialRefund.get("id") + "/fail", workspaceHeaders("partial-refund-fail-kay007", "kay007-alpha", "kay007-owner"), Map.of("failureReason", "provider correction"));
        post("/api/payments/refunds/" + partialRefund.get("id") + "/retry", workspaceHeaders("partial-refund-retry-kay007", "kay007-alpha", "kay007-owner"), Map.of());
        assertThat(fullRefund.get("refundType")).isEqualTo("FULL");
        assertThat(partialRefund.get("refundType")).isEqualTo("PARTIAL");
        assertThat(reversal.get("refundType")).isEqualTo("REVERSAL");

        Map<String, Object> dispute = post("/api/payments/disputes", workspaceHeaders("dispute-open-kay007", "kay007-alpha", "kay007-owner"), Map.of("paymentIntentId", secondPaymentId, "amountMinor", 1000));
        String disputeId = (String) dispute.get("id");
        Map<String, Object> frozenSummary = balanceSummary(ownerHeaders);
        assertThat(frozenSummary.get("frozenAmountMinor")).isEqualTo(1000);
        post("/api/payments/disputes/" + disputeId + "/resolve", workspaceHeaders("dispute-resolve-kay007", "kay007-alpha", "kay007-owner"), Map.of("resolution", "WON", "note", "provider won"));
        Map<String, Object> releasedSummary = balanceSummary(ownerHeaders);
        assertThat(releasedSummary.get("frozenAmountMinor")).isEqualTo(0);

        Map<String, Object> lostDispute = post("/api/payments/disputes", workspaceHeaders("dispute-lost-open-kay007", "kay007-alpha", "kay007-owner"), Map.of("paymentIntentId", secondPaymentId, "amountMinor", 500));
        post("/api/payments/disputes/" + lostDispute.get("id") + "/resolve", workspaceHeaders("dispute-lost-resolve-kay007", "kay007-alpha", "kay007-owner"), Map.of("resolution", "LOST"));
        Map<String, Object> finalSummary = balanceSummary(ownerHeaders);
        assertThat(finalSummary.get("payableAmountMinor")).isEqualTo(21600);
        assertThat(finalSummary.get("paidOutAmountMinor")).isEqualTo(5000);
        assertThat(finalSummary.get("refundedAmountMinor")).isEqualTo(12300);
        assertThat(finalSummary.get("availableAmountMinor")).isEqualTo(3800);

        List<?> payoutJournals = getList("/api/finance/journal-entries/by-reference?referenceType=PAYOUT&referenceId=" + payoutId, ownerHeaders, HttpStatus.OK);
        assertThat(payoutJournals).hasSize(4);
        payoutJournals.forEach(entry -> assertBalanced((Map<?, ?>) entry));
        List<?> refundJournals = getList("/api/finance/journal-entries/by-reference?referenceType=REFUND&referenceId=" + fullRefund.get("id"), ownerHeaders, HttpStatus.OK);
        assertThat(refundJournals).hasSize(1);
        refundJournals.forEach(entry -> assertBalanced((Map<?, ?>) entry));
        List<?> disputeJournals = getList("/api/finance/journal-entries/by-reference?referenceType=DISPUTE&referenceId=" + disputeId, ownerHeaders, HttpStatus.OK);
        assertThat(disputeJournals).hasSize(2);
        disputeJournals.forEach(entry -> assertBalanced((Map<?, ?>) entry));

        assertThat(getList("/api/payments/payouts", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/payments/refunds", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/payments/disputes", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/finance/journal-entries/by-reference?referenceType=PAYOUT&referenceId=" + payoutId, betaHeaders, HttpStatus.OK)).isEmpty();
    }

    private String settledPayment(Object offeringId, Object customerProfileId, String start, String end, String suffix, HttpHeaders ownerHeaders) {
        String paymentId = paymentForHeldBooking(offeringId, customerProfileId, start, end, suffix);
        post("/api/payments/intents/" + paymentId + "/authorize", workspaceHeaders("authorize-" + suffix + "-kay007", "kay007-alpha", "kay007-owner"), Map.of("amountMinor", 12000));
        post("/api/payments/intents/" + paymentId + "/capture", workspaceHeaders("capture-" + suffix + "-kay007", "kay007-alpha", "kay007-owner"), Map.of("amountMinor", 12000));
        post("/api/payments/intents/" + paymentId + "/settle", workspaceHeaders("settle-" + suffix + "-kay007", "kay007-alpha", "kay007-owner"), Map.of("amountMinor", 12000));
        List<?> journals = getList("/api/finance/journal-entries/by-reference?referenceType=PAYMENT&referenceId=" + paymentId, ownerHeaders, HttpStatus.OK);
        assertThat(journals).hasSize(3);
        journals.forEach(entry -> assertBalanced((Map<?, ?>) entry));
        return paymentId;
    }

    private String paymentForHeldBooking(Object offeringId, Object customerProfileId, String start, String end, String suffix) {
        Map<String, Object> booking = post("/api/bookings", workspaceHeaders("booking-" + suffix + "-kay007", "kay007-alpha", "kay007-customer"),
                scheduledBooking(offeringId, customerProfileId, start, end));
        String bookingId = (String) ((Map<?, ?>) booking.get("booking")).get("id");
        Map<String, Object> intent = post("/api/payments/intents", workspaceHeaders("intent-" + suffix + "-kay007", "kay007-alpha", "kay007-owner"), Map.of("bookingId", bookingId));
        return (String) ((Map<?, ?>) intent.get("paymentIntent")).get("id");
    }

    private Map<String, Object> balanceSummary(HttpHeaders headers) {
        List<?> summaries = getList("/api/finance/provider-balance-summary", headers, HttpStatus.OK);
        assertThat(summaries).hasSize(1);
        return asMap((Map<?, ?>) summaries.get(0));
    }

    private String accountIdByPurpose(HttpHeaders headers, String purpose) {
        return getList("/api/finance/accounts", headers, HttpStatus.OK).stream()
                .map(value -> (Map<?, ?>) value)
                .filter(account -> purpose.equals(account.get("accountPurpose")))
                .map(account -> (String) account.get("id"))
                .findFirst()
                .orElseThrow();
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

    private Map<String, Object> account(String code, String name, String type, String purpose) {
        return Map.of("accountCode", code, "accountName", name, "accountType", type, "accountPurpose", purpose, "currencyCode", "USD");
    }

    private Map<String, Object> scheduledOffering(Object providerProfileId) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", "KAY007 Session"),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed-plus-unit")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "kay007")),
                Map.entry("pricingRules", List.of(
                        Map.of("ruleType", "FIXED_PRICE", "currencyCode", "USD", "amountMinor", 10000, "sortOrder", 0),
                        Map.of("ruleType", "PER_UNIT", "currencyCode", "USD", "amountMinor", 2000, "unitName", "attendee", "sortOrder", 1))),
                Map.entry("availabilityWindows", List.of(Map.of("weekday", 1, "startLocalTime", "09:00:00", "endLocalTime", "17:00:00"))));
    }

    private Map<String, Object> scheduledBooking(Object offeringId, Object customerProfileId, String start, String end) {
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
