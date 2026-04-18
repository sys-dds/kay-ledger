package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kayledger.api.shared.messaging.application.OutboxService;
import com.kayledger.api.shared.messaging.model.OutboxEvent;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Kay008RecoveryKay009AsyncBackboneIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_test")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.test.events");
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    OutboxService outboxService;

    @Test
    void kay008RecoveryAndKay009AsyncBackboneWorkEndToEnd() throws Exception {
        post("/api/workspaces", headers("ws-async-alpha"), Map.of("slug", "async-alpha", "displayName", "Async Alpha"));
        post("/api/workspaces", headers("ws-async-beta"), Map.of("slug", "async-beta", "displayName", "Async Beta"));

        Map<String, Object> owner = post("/api/actors", headers("actor-async-owner"), actor("async-owner", "Async Owner"));
        Map<String, Object> provider = post("/api/actors", headers("actor-async-provider"), actor("async-provider", "Async Provider"));
        Map<String, Object> customer = post("/api/actors", headers("actor-async-customer"), actor("async-customer", "Async Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("actor-async-beta"), actor("async-beta-owner", "Async Beta Owner"));

        post("/api/memberships", headers("member-async-owner"), Map.of("workspaceSlug", "async-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("member-async-provider", "async-alpha", "async-owner"), Map.of("workspaceSlug", "async-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("member-async-customer", "async-alpha", "async-owner"), Map.of("workspaceSlug", "async-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("member-async-beta"), Map.of("workspaceSlug", "async-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("async-read-owner", "async-alpha", "async-owner");
        HttpHeaders betaHeaders = workspaceHeaders("async-read-beta", "async-beta", "async-beta-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("profile-async-provider", "async-alpha", "async-owner"), Map.of("actorId", provider.get("id"), "displayName", "Async Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("profile-async-customer", "async-alpha", "async-owner"), Map.of("actorId", customer.get("id"), "displayName", "Async Customer"));

        createAccounts();

        Map<String, Object> monthly = post("/api/subscriptions/plans", workspaceHeaders("async-plan-monthly", "async-alpha", "async-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "planCode", "ASYNC_MONTHLY",
                "displayName", "Async Monthly",
                "billingInterval", "MONTHLY",
                "currencyCode", "USD",
                "amountMinor", 3000));
        Map<String, Object> yearly = post("/api/subscriptions/plans", workspaceHeaders("async-plan-yearly", "async-alpha", "async-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "planCode", "ASYNC_YEARLY",
                "displayName", "Async Yearly",
                "billingInterval", "YEARLY",
                "currencyCode", "USD",
                "amountMinor", 30000));

        assertThat(getList("/api/messaging/outbox", ownerHeaders, HttpStatus.OK)).isNotEmpty();

        Map<String, Object> subscription = post("/api/subscriptions", workspaceHeaders("async-sub-create", "async-alpha", "async-owner"), Map.of(
                "customerProfileId", customerProfile.get("id"),
                "planId", monthly.get("id"),
                "startAt", "2035-01-01T00:00:00Z"));
        String subscriptionId = (String) subscription.get("id");
        assertThat(subscription.get("status")).isEqualTo("PENDING_ACTIVATION");
        List<?> cycles = getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK);
        Map<?, ?> firstCycle = (Map<?, ?>) cycles.get(0);
        assertThat(firstCycle.get("status")).isEqualTo("PENDING_PAYMENT");
        assertThat(firstCycle.get("grossAmountMinor")).isEqualTo(3000);
        String firstCycleId = (String) firstCycle.get("id");
        String firstPaymentId = paymentIntentIdForCycle(ownerHeaders, firstCycleId);

        runRelay(ownerHeaders, "async-relay-1");
        awaitProjection("/api/payments/projections", ownerHeaders, "payment_intent_id", firstPaymentId);
        assertThat(getList("/api/payments/intents/by-subscription-cycle/" + firstCycleId, ownerHeaders, HttpStatus.OK).get(0))
                .extracting(value -> ((Map<?, ?>) value).get("subscriptionCycleId"))
                .isEqualTo(firstCycleId);

        settleSubscriptionPayment(firstPaymentId, 3000, "async-initial");
        runRelay(ownerHeaders, "async-relay-2");
        awaitProjection("/api/subscriptions/projections", ownerHeaders, "subscription_id", subscriptionId);
        assertThat(getList("/api/subscriptions", ownerHeaders, HttpStatus.OK).stream()
                .map(value -> (Map<?, ?>) value)
                .filter(value -> subscriptionId.equals(value.get("id")))
                .findFirst()
                .orElseThrow()
                .get("status")).isEqualTo("ACTIVE");

        post("/api/subscriptions/renewals/run", workspaceHeaders("async-renew-run", "async-alpha", "async-owner"), Map.of("now", "2035-01-31T00:00:00Z"));
        Map<String, Object> duplicateRenewal = post("/api/subscriptions/renewals/run", workspaceHeaders("async-renew-again", "async-alpha", "async-owner"), Map.of("now", "2035-01-31T00:00:00Z"));
        assertThat(duplicateRenewal.get("paymentIntentsCreated")).isEqualTo(0);
        List<?> renewalCycles = getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK);
        assertThat(renewalCycles).hasSize(2);

        String secondCycleId = (String) ((Map<?, ?>) renewalCycles.get(1)).get("id");
        post("/api/subscriptions/" + subscriptionId + "/plan-changes", workspaceHeaders("async-plan-change", "async-alpha", "async-owner"), Map.of(
                "targetPlanId", yearly.get("id"),
                "effectiveCycleNumber", 3));
        post("/api/subscriptions/renewals/run", workspaceHeaders("async-renew-fail", "async-alpha", "async-owner"), Map.of("now", "2035-03-02T00:00:00Z", "forceFailure", true));
        Map<String, Object> repeatedFailure = post("/api/subscriptions/renewals/run", workspaceHeaders("async-renew-fail-again", "async-alpha", "async-owner"), Map.of("now", "2035-03-02T00:00:00Z", "forceFailure", true));
        assertThat(repeatedFailure.get("subscriptionsProcessed")).isEqualTo(0);
        assertThat(getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK)).hasSize(2);

        settleSubscriptionPayment(paymentIntentIdForCycle(ownerHeaders, secondCycleId), 3000, "async-renewal");
        runRelay(ownerHeaders, "async-relay-3");
        assertThat(getList("/api/subscriptions/projections", ownerHeaders, HttpStatus.OK)).isNotEmpty();
        assertThat(getList("/api/subscriptions/projections", betaHeaders, HttpStatus.OK)).isEmpty();

        OutboxEvent parked = outboxService.append(UUID.fromString((String) subscription.get("workspaceId")), "SUBSCRIPTION", UUID.fromString(subscriptionId), "subscription.synthetic.failure", "synthetic-failure:" + subscriptionId, Map.of("subscriptionId", subscriptionId, "status", "ACTIVE"));
        outboxService.recordFailure(parked.id(), new IllegalStateException("forced failure one"));
        outboxService.recordFailure(parked.id(), new IllegalStateException("forced failure two"));
        assertThat(getList("/api/messaging/outbox/parked", ownerHeaders, HttpStatus.OK)).isNotEmpty();
        post("/api/messaging/outbox/parked/" + parked.id() + "/replay", workspaceHeaders("async-replay-parked", "async-alpha", "async-owner"), Map.of());
        assertThat(getList("/api/messaging/outbox/parked", ownerHeaders, HttpStatus.OK)).isEmpty();

        runRelay(ownerHeaders, "async-relay-4");
        runRelay(ownerHeaders, "async-relay-duplicate");
        assertThat(getList("/api/payments/projections", ownerHeaders, HttpStatus.OK)).isNotEmpty();
    }

    private void runRelay(HttpHeaders headers, String key) throws InterruptedException {
        HttpHeaders relayHeaders = new HttpHeaders();
        relayHeaders.putAll(headers);
        relayHeaders.set("Idempotency-Key", key);
        post("/api/messaging/outbox/relay", relayHeaders, Map.of());
        Thread.sleep(800);
    }

    private void awaitProjection(String path, HttpHeaders headers, String idColumn, String id) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            boolean found = getList(path, headers, HttpStatus.OK).stream()
                    .map(value -> (Map<?, ?>) value)
                    .anyMatch(value -> id.equals(String.valueOf(value.get(idColumn))));
            if (found) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Projection row was not created for " + id);
    }

    private void createAccounts() {
        post("/api/finance/accounts", workspaceHeaders("acct-auth-async", "async-alpha", "async-owner"), account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-platform-async", "async-alpha", "async-owner"), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("acct-captured-async", "async-alpha", "async-owner"), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-seller-async", "async-alpha", "async-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("acct-fee-async", "async-alpha", "async-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        post("/api/finance/accounts", workspaceHeaders("acct-cash-async", "async-alpha", "async-owner"), account("1000", "Cash Placeholder", "ASSET", "CASH_PLACEHOLDER"));
    }

    private String paymentIntentIdForCycle(HttpHeaders headers, String cycleId) {
        List<?> intents = getList("/api/payments/intents/by-subscription-cycle/" + cycleId, headers, HttpStatus.OK);
        assertThat(intents).hasSize(1);
        return (String) ((Map<?, ?>) intents.get(0)).get("id");
    }

    private void settleSubscriptionPayment(String paymentIntentId, int amountMinor, String keyPrefix) {
        post("/api/payments/intents/" + paymentIntentId + "/authorize", workspaceHeaders(keyPrefix + "-auth", "async-alpha", "async-owner"), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentIntentId + "/capture", workspaceHeaders(keyPrefix + "-capture", "async-alpha", "async-owner"), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentIntentId + "/settle", workspaceHeaders(keyPrefix + "-settle", "async-alpha", "async-owner"), Map.of("amountMinor", amountMinor));
    }

    private Map<String, Object> actor(String actorKey, String displayName) {
        return Map.of("actorKey", actorKey, "displayName", displayName);
    }

    private Map<String, Object> account(String code, String name, String type, String purpose) {
        return Map.of("accountCode", code, "accountName", name, "accountType", type, "accountPurpose", purpose, "currencyCode", "USD");
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return asMap(response.getBody());
    }

    private List<?> getList(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<List> response = exchange(path, HttpMethod.GET, withoutIdempotency(headers), null, List.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody();
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
