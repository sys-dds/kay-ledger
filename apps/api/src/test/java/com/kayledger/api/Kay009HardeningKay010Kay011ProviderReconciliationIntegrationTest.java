package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kayledger.api.shared.messaging.application.InboxService;
import com.kayledger.api.shared.messaging.application.ProjectionConsumer;
import com.kayledger.api.shared.messaging.store.OutboxStore;
import com.kayledger.api.temporal.config.TemporalProperties;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;

import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Kay009HardeningKay010Kay011ProviderReconciliationIntegrationTest {

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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay010.test.events");
        registry.add("kay-ledger.temporal.task-queues.operator-workflows", () -> "kay-ledger-kay009-legacy");
        registry.add("kay-ledger.async.relay.max-attempts", () -> "2");
        registry.add("kay-ledger.async.relay.backoff-seconds", () -> "1");
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    InboxService inboxService;

    @Autowired
    ProjectionConsumer projectionConsumer;

    @Autowired
    OutboxStore outboxStore;

    @Test
    void kay009RecoveryKay010Kay011WorkEndToEnd() {
        post("/api/workspaces", headers("k10-ws-alpha"), Map.of("slug", "k10-alpha", "displayName", "K10 Alpha"));
        post("/api/workspaces", headers("k10-ws-beta"), Map.of("slug", "k10-beta", "displayName", "K10 Beta"));
        Map<String, Object> owner = post("/api/actors", headers("k10-owner"), actor("k10-owner", "K10 Owner"));
        Map<String, Object> provider = post("/api/actors", headers("k10-provider"), actor("k10-provider", "K10 Provider"));
        Map<String, Object> customer = post("/api/actors", headers("k10-customer"), actor("k10-customer", "K10 Customer"));
        Map<String, Object> betaOwner = post("/api/actors", headers("k10-beta-owner"), actor("k10-beta-owner", "K10 Beta"));
        post("/api/memberships", headers("k10-member-owner"), Map.of("workspaceSlug", "k10-alpha", "actorId", owner.get("id"), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("k10-member-provider", "k10-alpha", "k10-owner"), Map.of("workspaceSlug", "k10-alpha", "actorId", provider.get("id"), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("k10-member-customer", "k10-alpha", "k10-owner"), Map.of("workspaceSlug", "k10-alpha", "actorId", customer.get("id"), "role", "CUSTOMER"));
        post("/api/memberships", headers("k10-member-beta"), Map.of("workspaceSlug", "k10-beta", "actorId", betaOwner.get("id"), "role", "OWNER"));

        HttpHeaders ownerHeaders = workspaceHeaders("k10-read", "k10-alpha", "k10-owner");
        HttpHeaders betaHeaders = workspaceHeaders("k10-beta-read", "k10-beta", "k10-beta-owner");
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("k10-provider-profile", "k10-alpha", "k10-owner"), Map.of("actorId", provider.get("id"), "displayName", "K10 Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("k10-customer-profile", "k10-alpha", "k10-owner"), Map.of("actorId", customer.get("id"), "displayName", "K10 Customer"));
        createAccounts();

        Map<String, Object> plan = post("/api/subscriptions/plans", workspaceHeaders("k10-plan", "k10-alpha", "k10-owner"), Map.of(
                "providerProfileId", providerProfile.get("id"),
                "planCode", "K10_MONTHLY",
                "displayName", "K10 Monthly",
                "billingInterval", "MONTHLY",
                "currencyCode", "USD",
                "amountMinor", 1800));
        Map<String, Object> subscription = post("/api/subscriptions", workspaceHeaders("k10-sub", "k10-alpha", "k10-owner"), Map.of(
                "customerProfileId", customerProfile.get("id"),
                "planId", plan.get("id"),
                "startAt", "2036-01-01T00:00:00Z"));
        String subscriptionId = (String) subscription.get("id");
        String firstCycleId = (String) ((Map<?, ?>) getList("/api/subscriptions/" + subscriptionId + "/cycles", ownerHeaders, HttpStatus.OK).get(0)).get("id");
        String firstPaymentId = paymentIntentIdForCycle(ownerHeaders, firstCycleId);

        String replayPayload = jdbcTemplate.queryForObject("SELECT payload_json::text FROM outbox_events WHERE workspace_id = ? ORDER BY created_at DESC LIMIT 1", String.class, workspaceId("k10-alpha"));
        assertThatThrownBy(() -> inboxService.processOnce(workspaceId("k10-alpha"), "topic", 0, "key", UUID.randomUUID(), "consumer-failure-1", "projection-consumer", replayPayload, () -> {
            throw new IllegalStateException("forced projection failure");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(inboxOutcome("consumer-failure-1")).isEqualTo("FAILED");
        jdbcTemplate.update("UPDATE inbox_messages SET available_at = now() WHERE dedupe_key = 'consumer-failure-1'");
        inboxService.processOnce(workspaceId("k10-alpha"), "topic", 0, "key", UUID.randomUUID(), "consumer-failure-1", "projection-consumer", replayPayload, () -> {
            throw new IllegalStateException("forced projection failure again");
        });
        assertThat(inboxOutcome("consumer-failure-1")).isEqualTo("PARKED");
        assertThat(getList("/api/messaging/inbox/parked", ownerHeaders, HttpStatus.OK)).isNotEmpty();
        post("/api/messaging/inbox/parked/projection-consumer/consumer-failure-1/replay", workspaceHeaders("k10-inbox-replay", "k10-alpha", "k10-owner"), Map.of());
        assertThat(inboxOutcome("consumer-failure-1")).isEqualTo("PROCESSED");

        String payload = jdbcTemplate.queryForObject("SELECT payload_json::text FROM outbox_events WHERE workspace_id = ? ORDER BY created_at DESC LIMIT 1", String.class, workspaceId("k10-alpha"));
        projectionConsumer.consume(new ConsumerRecord<>("kay-ledger.kay010.test.events", 0, 1, "dupe", payload));
        projectionConsumer.consume(new ConsumerRecord<>("kay-ledger.kay010.test.events", 0, 2, "dupe", payload));
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM inbox_messages WHERE message_key = 'dupe'", Integer.class)).isEqualTo(1);

        post("/api/payments/intents/" + firstPaymentId + "/fail", workspaceHeaders("k10-real-fail", "k10-alpha", "k10-owner"), Map.of());
        assertThat(subscriptionStatus(subscriptionId, ownerHeaders)).isEqualTo("GRACE");

        post("/api/providers/configs", workspaceHeaders("k10-provider-config", "k10-alpha", "k10-owner"), Map.of(
                "providerKey", "internal-test",
                "displayName", "Internal Test",
                "signingSecret", "secret"));
        Map<String, Object> paymentCallback = callback("evt-1", 10, "PAYMENT_FAILED", firstPaymentId, 0);
        HttpHeaders callbackHeaders = workspaceHeaders("ignored", "k10-alpha", "k10-owner");
        callbackHeaders.set("X-Provider-Signature", signature("secret", paymentCallback));
        Map<String, Object> applied = post("/api/providers/internal-test/callbacks", callbackHeaders, paymentCallback);
        Map<String, Object> duplicate = post("/api/providers/internal-test/callbacks", callbackHeaders, paymentCallback);
        assertThat(applied.get("processingStatus")).isEqualTo("APPLIED");
        assertThat(duplicate.get("processingStatus")).isIn("APPLIED", "DUPLICATE");

        Map<String, Object> staleCallback = callback("evt-stale", 9, "PAYMENT_AUTHORIZED", firstPaymentId, 0);
        callbackHeaders.set("X-Provider-Signature", signature("secret", staleCallback));
        assertThat(post("/api/providers/internal-test/callbacks", callbackHeaders, staleCallback).get("processingStatus")).isEqualTo("IGNORED_OUT_OF_ORDER");

        jdbcTemplate.update("UPDATE payment_intents SET status = 'CREATED' WHERE id = ?::uuid", firstPaymentId);
        post("/api/reconciliation/runs", workspaceHeaders("k10-recon", "k10-alpha", "k10-owner"), Map.of("runType", "PAYMENT"));
        awaitLatestRunCompleted();
        List<?> mismatches = getList("/api/reconciliation/mismatches", ownerHeaders, HttpStatus.OK);
        assertThat(mismatches).isNotEmpty();
        Map<?, ?> mismatch = (Map<?, ?>) mismatches.get(0);
        assertThat(mismatch.get("driftCategory")).isEqualTo("STATE_MISMATCH");
        String mismatchId = (String) mismatch.get("id");
        post("/api/reconciliation/mismatches/" + mismatchId + "/mark-repair", workspaceHeaders("k10-mark-repair", "k10-alpha", "k10-owner"), Map.of("note", "operator reviewed"));
        post("/api/reconciliation/mismatches/" + mismatchId + "/apply-repair", workspaceHeaders("k10-apply-repair", "k10-alpha", "k10-owner"), Map.of("note", "provider truth applied"));
        assertThat(paymentStatus(firstPaymentId)).isEqualTo("FAILED");

        assertThat(getList("/api/providers/callbacks", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/reconciliation/mismatches", betaHeaders, HttpStatus.OK)).isEmpty();
    }

    private UUID workspaceId(String slug) {
        return jdbcTemplate.queryForObject("SELECT id FROM workspaces WHERE slug = ?", UUID.class, slug);
    }

    private String inboxOutcome(String dedupeKey) {
        return jdbcTemplate.queryForObject("SELECT outcome FROM inbox_messages WHERE dedupe_key = ?", String.class, dedupeKey);
    }

    private String paymentStatus(String paymentIntentId) {
        return jdbcTemplate.queryForObject("SELECT status FROM payment_intents WHERE id = ?::uuid", String.class, paymentIntentId);
    }

    private String subscriptionStatus(String subscriptionId, HttpHeaders headers) {
        return getList("/api/subscriptions", headers, HttpStatus.OK).stream()
                .map(value -> (Map<?, ?>) value)
                .filter(value -> subscriptionId.equals(value.get("id")))
                .findFirst()
                .orElseThrow()
                .get("status")
                .toString();
    }

    private String paymentIntentIdForCycle(HttpHeaders headers, String cycleId) {
        return (String) ((Map<?, ?>) getList("/api/payments/intents/by-subscription-cycle/" + cycleId, headers, HttpStatus.OK).get(0)).get("id");
    }

    private void createAccounts() {
        post("/api/finance/accounts", workspaceHeaders("k10-acct-auth", "k10-alpha", "k10-owner"), account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("k10-acct-platform", "k10-alpha", "k10-owner"), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("k10-acct-captured", "k10-alpha", "k10-owner"), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("k10-acct-seller", "k10-alpha", "k10-owner"), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("k10-acct-fee", "k10-alpha", "k10-owner"), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        post("/api/finance/accounts", workspaceHeaders("k10-acct-cash", "k10-alpha", "k10-owner"), account("1000", "Cash Placeholder", "ASSET", "CASH_PLACEHOLDER"));
    }

    private Map<String, Object> callback(String eventId, long sequence, String type, String referenceId, long amountMinor) {
        Map<String, Object> callback = new LinkedHashMap<>();
        callback.put("providerEventId", eventId);
        callback.put("providerSequence", sequence);
        callback.put("callbackType", type);
        callback.put("businessReferenceId", referenceId);
        callback.put("amountMinor", amountMinor);
        callback.put("metadata", null);
        return callback;
    }

    private String signature(String secret, Map<String, Object> callback) {
        return sign(secret, "{\"providerEventId\":\"" + callback.get("providerEventId")
                + "\",\"providerSequence\":" + callback.get("providerSequence")
                + ",\"callbackType\":\"" + callback.get("callbackType")
                + "\",\"businessReferenceId\":\"" + callback.get("businessReferenceId")
                + "\",\"amountMinor\":" + callback.get("amountMinor")
                + ",\"metadata\":null}");
    }

    private static String sign(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
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
        return (Map<String, Object>) response.getBody();
    }

    private List<?> getList(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<List> response = exchange(path, HttpMethod.GET, withoutIdempotency(headers), null, List.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody();
    }

    private void awaitLatestRunCompleted() {
        long deadline = System.currentTimeMillis() + 20_000;
        String status = null;
        while (System.currentTimeMillis() < deadline) {
            status = jdbcTemplate.queryForObject("""
                    SELECT status FROM reconciliation_runs
                    WHERE workspace_id = ?::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                    """, String.class, workspaceId("k10-alpha"));
            if ("COMPLETED".equals(status)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(exception);
            }
        }
        assertThat(status).isEqualTo("COMPLETED");
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

    @TestConfiguration
    static class TemporalTestConfiguration {

        @Bean(destroyMethod = "")
        TestWorkflowEnvironment testWorkflowEnvironment() {
            return TestWorkflowEnvironment.newInstance();
        }

        @Bean
        WorkflowClient workflowClient(TestWorkflowEnvironment environment) {
            return environment.getWorkflowClient();
        }

        @Bean
        WorkerFactory workerFactory(TestWorkflowEnvironment environment) {
            return environment.getWorkerFactory();
        }

        @Bean
        Worker testOperatorWorker(WorkerFactory workerFactory, TemporalProperties properties, List<TemporalWorkerCustomizer> customizers) {
            Worker worker = workerFactory.newWorker(properties.getTaskQueues().getOperatorWorkflows());
            customizers.forEach(customizer -> customizer.customize(worker));
            return worker;
        }

        @Bean
        SmartLifecycle temporalTestLifecycle(TestWorkflowEnvironment environment) {
            return new SmartLifecycle() {
                private boolean running;

                @Override
                public void start() {
                    environment.start();
                    running = true;
                }

                @Override
                public void stop() {
                    environment.close();
                    running = false;
                }

                @Override
                public boolean isRunning() {
                    return running;
                }
            };
        }
    }
}
