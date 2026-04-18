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
class Kay007Kay008SubscriptionsRecurringBillingIntegrationTest {

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
    void kay007RecoveryAndKay008SubscriptionsRecurringBillingWorkEndToEnd() {
        post("/api/workspaces", headers("ws-sub-alpha"), Map.of("slug", "subs-alpha", "displayName", "Subs Alpha"));
        post("/api/workspaces", headers("ws-sub-beta"), Map.of("slug", "subs-beta", "displayName", "Subs Beta"));

        Map<String, Object> owner = post("/api/actors", headers("actor-sub-owner"), actor("subs-owner", "Subs Owner"));
        Map<String, Object> provider = post("/api/actors", headers("actor-sub-provider"), actor("subs-provider", "Subs Provider"));
        Map<String, Object> customer = post("/api/actors", headers("actor-sub-customer"), actor("subs-customer", "Subs Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("actor-sub-beta"), actor("subs-beta-owner", "Subs Beta Owner"));

        post("/api/memberships", headers("member-sub-owner"), Map.of("workspaceSlug", "subs-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("member-sub-provider", "subs-alpha", "subs-owner"), Map.of("workspaceSlug", "subs-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("member-sub-customer", "subs-alpha", "subs-owner"), Map.of("workspaceSlug", "subs-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("member-sub-beta"), Map.of("workspaceSlug", "subs-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("owner-read-subs", "subs-alpha", "subs-owner");
        HttpHeaders betaHeaders = workspaceHeaders("beta-read-subs", "subs-beta", "subs-beta-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("profile-sub-provider", "subs-alpha", "subs-owner"), Map.of("actorId", provider.get("id"), "displayName", "Subs Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("profile-sub-customer", "subs-alpha", "subs-owner"), Map.of("actorId", customer.get("id"), "displayName", "Subs Customer"));

        createAccounts();
        Map<String, Object> offering = post("/api/offerings", workspaceHeaders("offering-subs", "subs-alpha", "subs-owner"), scheduledOffering(providerProfile.get("id")));
        String offeringId = offeringId(offering);
        post("/api/offerings/" + offeringId + "/publish", workspaceHeaders("offering-subs-publish", "subs-alpha", "subs-owner"), Map.of());
        post("/api/finance/fee-rules", workspaceHeaders("fee-subs", "subs-alpha", "subs-owner"), Map.of(
                "offeringId", offeringId,
                "ruleType", "BASIS_POINTS",
                "basisPoints", 1000,
                "currencyCode", "USD"));

        String paymentId = settledPayment(offeringId, customerProfile.get("id"));
        Map<String, Object> capturedBalance = getMap("/api/finance/accounts/" + accountIdByPurpose(ownerHeaders, "CAPTURED_FUNDS") + "/balance", ownerHeaders, HttpStatus.OK);
        assertThat(capturedBalance.get("signedBalanceMinor")).isEqualTo(0);

        Map<String, Object> fullRefund = post("/api/payments/refunds/full", workspaceHeaders("sub-full-refund", "subs-alpha", "subs-owner"), Map.of("paymentIntentId", paymentId));
        assertStatus("/api/payments/disputes", HttpMethod.POST, workspaceHeaders("sub-double-dispute", "subs-alpha", "subs-owner"), Map.of("paymentIntentId", paymentId, "amountMinor", 1), HttpStatus.BAD_REQUEST);
        List<?> payableBalances = getList("/api/finance/payable-balances", ownerHeaders, HttpStatus.OK);
        List<?> balanceSummaries = getList("/api/finance/provider-balance-summary", ownerHeaders, HttpStatus.OK);
        assertThat(payableBalances).isEqualTo(balanceSummaries);
        assertThat(fullRefund.get("refundType")).isEqualTo("FULL");

        Map<String, Object> monthly = post("/api/subscriptions/plans", workspaceHeaders("plan-monthly", "subs-alpha", "subs-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "planCode", "MONTHLY",
                "displayName", "Monthly Access",
                "billingInterval", "MONTHLY",
                "currencyCode", "USD",
                "amountMinor", 2000));
        Map<String, Object> yearly = post("/api/subscriptions/plans", workspaceHeaders("plan-yearly", "subs-alpha", "subs-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "planCode", "YEARLY",
                "displayName", "Yearly Access",
                "billingInterval", "YEARLY",
                "currencyCode", "USD",
                "amountMinor", 20000));
        String startAt = "2032-01-01T00:00:00Z";
        Map<String, Object> subscription = post("/api/subscriptions", workspaceHeaders("subscription-create", "subs-alpha", "subs-owner"), Map.of(
                "customerProfileId", customerProfile.get("id"),
                "planId", monthly.get("id"),
                "startAt", startAt));
        String subscriptionId = (String) subscription.get("id");
        assertThat(subscription.get("status")).isEqualTo("PENDING_ACTIVATION");
        List<?> firstCycles = getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK);
        assertThat(firstCycles).hasSize(1);
        assertThat(((Map<?, ?>) firstCycles.get(0)).get("cycleNumber")).isEqualTo(1);
        assertThat(((Map<?, ?>) firstCycles.get(0)).get("status")).isEqualTo("PENDING_PAYMENT");
        String cycleOneId = (String) ((Map<?, ?>) firstCycles.get(0)).get("id");
        String initialSubscriptionPaymentId = paymentIntentIdForCycle(ownerHeaders, cycleOneId);
        settleSubscriptionPayment(initialSubscriptionPaymentId, 2000, "initial-sub");
        firstCycles = getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK);
        assertThat(((Map<?, ?>) firstCycles.get(0)).get("status")).isEqualTo("PAID");
        assertThat(getList("/api/subscriptions", ownerHeaders, HttpStatus.OK).stream()
                .map(value -> (Map<?, ?>) value)
                .filter(value -> subscriptionId.equals(value.get("id")))
                .findFirst()
                .orElseThrow()
                .get("status")).isEqualTo("ACTIVE");
        assertEntitlement(ownerHeaders, subscriptionId, "ACTIVE");

        Map<String, Object> renewal = post("/api/subscriptions/renewals/run", workspaceHeaders("renew-run", "subs-alpha", "subs-owner"), Map.of("now", "2032-02-01T00:00:00Z"));
        assertThat(renewal.get("subscriptionsProcessed")).isEqualTo(1);
        assertThat(renewal.get("paymentIntentsCreated")).isEqualTo(1);
        Map<String, Object> renewalReplay = post("/api/subscriptions/renewals/run", workspaceHeaders("renew-run", "subs-alpha", "subs-owner"), Map.of("now", "2032-02-01T00:00:00Z"));
        assertThat(renewalReplay).isEqualTo(renewal);
        Map<String, Object> renewalAgain = post("/api/subscriptions/renewals/run", workspaceHeaders("renew-run-again", "subs-alpha", "subs-owner"), Map.of("now", "2032-02-01T00:00:00Z"));
        assertThat(renewalAgain.get("paymentIntentsCreated")).isEqualTo(0);
        List<?> secondCycles = getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK);
        assertThat(secondCycles).hasSize(2);
        String cycleTwoId = (String) ((Map<?, ?>) secondCycles.get(1)).get("id");
        assertThat(((Map<?, ?>) secondCycles.get(1)).get("status")).isEqualTo("PENDING_PAYMENT");
        assertThat(getList("/api/payments/intents/by-subscription-cycle/" + cycleTwoId, ownerHeaders, HttpStatus.OK)).hasSize(1);
        assertThat(((Map<?, ?>) getList("/api/payments/intents/by-subscription-cycle/" + cycleTwoId, ownerHeaders, HttpStatus.OK).get(0)).get("subscriptionCycleId")).isEqualTo(cycleTwoId);
        assertThat(getList("/api/payments/intents/by-subscription/" + subscriptionId, ownerHeaders, HttpStatus.OK)).hasSize(2);
        settleSubscriptionPayment(paymentIntentIdForCycle(ownerHeaders, cycleTwoId), 2000, "renewal-sub");

        post("/api/subscriptions/" + subscriptionId + "/plan-changes", workspaceHeaders("plan-change", "subs-alpha", "subs-owner"), Map.of(
                "targetPlanId", yearly.get("id"),
                "effectiveCycleNumber", 3));
        Map<String, Object> failedRenewal = post("/api/subscriptions/renewals/run", workspaceHeaders("renew-fail", "subs-alpha", "subs-owner"), Map.of(
                "now", "2032-03-02T00:00:00Z",
                "forceFailure", true));
        assertThat(failedRenewal.get("subscriptionsProcessed")).isEqualTo(1);
        assertEntitlement(ownerHeaders, subscriptionId, "GRACE");
        Map<String, Object> failedRenewalAgain = post("/api/subscriptions/renewals/run", workspaceHeaders("renew-fail-again", "subs-alpha", "subs-owner"), Map.of(
                "now", "2032-03-02T00:00:00Z",
                "forceFailure", true));
        assertThat(failedRenewalAgain.get("subscriptionsProcessed")).isEqualTo(0);
        assertThat(getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK)).hasSize(3);
        Map<String, Object> suspended = post("/api/subscriptions/suspensions/run", workspaceHeaders("suspend-run", "subs-alpha", "subs-owner"), Map.of("now", "2032-03-10T00:00:00Z"));
        assertThat(suspended.get("subscriptionsSuspended")).isEqualTo(1);
        assertEntitlement(ownerHeaders, subscriptionId, "SUSPENDED");

        Map<String, Object> cancelSubscription = post("/api/subscriptions", workspaceHeaders("subscription-cancel-create", "subs-alpha", "subs-owner"), Map.of(
                "customerProfileId", customerProfile.get("id"),
                "planId", yearly.get("id"),
                "startAt", "2034-01-01T00:00:00Z"));
        String cancelSubscriptionId = (String) cancelSubscription.get("id");
        Map<String, Object> cancelled = post("/api/subscriptions/" + cancelSubscriptionId + "/cancel", workspaceHeaders("subscription-cancel", "subs-alpha", "subs-owner"), Map.of());
        assertThat(cancelled.get("status")).isEqualTo("CANCELLED");
        assertEntitlement(ownerHeaders, cancelSubscriptionId, "CANCELLED");

        assertThat(getList("/api/subscriptions", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/payments/intents/by-subscription/" + subscriptionId, betaHeaders, HttpStatus.OK)).isEmpty();
    }

    private void createAccounts() {
        post("/api/finance/accounts", workspaceHeaders("acct-auth-subs", "subs-alpha", "subs-owner"), account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-platform-subs", "subs-alpha", "subs-owner"), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("acct-captured-subs", "subs-alpha", "subs-owner"), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-seller-subs", "subs-alpha", "subs-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("acct-fee-subs", "subs-alpha", "subs-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        post("/api/finance/accounts", workspaceHeaders("acct-cash-subs", "subs-alpha", "subs-owner"), account("1000", "Cash Placeholder", "ASSET", "CASH_PLACEHOLDER"));
        post("/api/finance/accounts", workspaceHeaders("acct-refund-subs", "subs-alpha", "subs-owner"), account("2200", "Refund Reserve", "LIABILITY", "REFUND_RESERVE"));
    }

    private String settledPayment(String offeringId, Object customerProfileId) {
        Map<String, Object> booking = post("/api/bookings", workspaceHeaders("booking-subs", "subs-alpha", "subs-customer"),
                scheduledBooking(offeringId, customerProfileId, "2031-01-06T10:00:00Z", "2031-01-06T11:00:00Z"));
        String bookingId = (String) ((Map<?, ?>) booking.get("booking")).get("id");
        Map<String, Object> intent = post("/api/payments/intents", workspaceHeaders("intent-subs", "subs-alpha", "subs-owner"), Map.of("bookingId", bookingId));
        String paymentId = (String) ((Map<?, ?>) intent.get("paymentIntent")).get("id");
        post("/api/payments/intents/" + paymentId + "/authorize", workspaceHeaders("auth-subs", "subs-alpha", "subs-owner"), Map.of("amountMinor", 12000));
        post("/api/payments/intents/" + paymentId + "/capture", workspaceHeaders("capture-subs", "subs-alpha", "subs-owner"), Map.of("amountMinor", 12000));
        post("/api/payments/intents/" + paymentId + "/settle", workspaceHeaders("settle-subs", "subs-alpha", "subs-owner"), Map.of("amountMinor", 12000));
        return paymentId;
    }

    private String paymentIntentIdForCycle(HttpHeaders headers, String cycleId) {
        List<?> intents = getList("/api/payments/intents/by-subscription-cycle/" + cycleId, headers, HttpStatus.OK);
        assertThat(intents).hasSize(1);
        return (String) ((Map<?, ?>) intents.get(0)).get("id");
    }

    private void settleSubscriptionPayment(String paymentIntentId, int amountMinor, String keyPrefix) {
        post("/api/payments/intents/" + paymentIntentId + "/authorize", workspaceHeaders(keyPrefix + "-auth", "subs-alpha", "subs-owner"), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentIntentId + "/capture", workspaceHeaders(keyPrefix + "-capture", "subs-alpha", "subs-owner"), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentIntentId + "/settle", workspaceHeaders(keyPrefix + "-settle", "subs-alpha", "subs-owner"), Map.of("amountMinor", amountMinor));
    }

    private void assertEntitlement(HttpHeaders headers, String subscriptionId, String status) {
        List<?> entitlements = getList("/api/subscriptions/" + subscriptionId + "/entitlements", headers, HttpStatus.OK);
        assertThat(entitlements).hasSize(1);
        assertThat(((Map<?, ?>) entitlements.get(0)).get("status")).isEqualTo(status);
    }

    private String accountIdByPurpose(HttpHeaders headers, String purpose) {
        return getList("/api/finance/accounts", headers, HttpStatus.OK).stream()
                .map(value -> (Map<?, ?>) value)
                .filter(account -> purpose.equals(account.get("accountPurpose")))
                .map(account -> (String) account.get("id"))
                .findFirst()
                .orElseThrow();
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
                Map.entry("title", "Subscription Proof Session"),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed-plus-unit")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 1800),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "kay008")),
                Map.entry("pricingRules", List.of(
                        Map.of("ruleType", "FIXED_PRICE", "currencyCode", "USD", "amountMinor", 10000, "sortOrder", 0),
                        Map.of("ruleType", "PER_UNIT", "currencyCode", "USD", "amountMinor", 2000, "unitName", "attendee", "sortOrder", 1))),
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
