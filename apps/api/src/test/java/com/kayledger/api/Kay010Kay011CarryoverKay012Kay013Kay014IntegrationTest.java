package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class Kay010Kay011CarryoverKay012Kay013Kay014IntegrationTest {

    private static final String ALPHA = "k12-alpha";
    private static final String BETA = "k12-beta";
    private static final String OWNER = "k12-owner";
    private static final String PROVIDER = "k12-provider";
    private static final String CUSTOMER = "k12-customer";
    private static final String BETA_OWNER = "k12-beta-owner";
    private static final String SECRET = "raw-callback-secret";
    private static final String BUCKET = "kay-ledger-test-exports";
    private static final String ACCESS_KEY = "kay_ledger";
    private static final String SECRET_KEY = "kay_ledger_secret";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_test")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @Container
    static final GenericContainer<?> OPENSEARCH = new GenericContainer<>(DockerImageName.parse("opensearchproject/opensearch:2.19.1"))
            .withEnv("discovery.type", "single-node")
            .withEnv("plugins.security.disabled", "true")
            .withEnv("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "KayLedger1!")
            .withExposedPorts(9200)
            .waitingFor(Wait.forHttp("/").forPort(9200).forStatusCode(200));

    @Container
    static final GenericContainer<?> MINIO = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server /data")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000).forStatusCode(200));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay012.test.events");
        registry.add("kay-ledger.async.relay.max-attempts", () -> "2");
        registry.add("kay-ledger.async.relay.backoff-seconds", () -> "1");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-investigation-it");
        registry.add("kay-ledger.object-storage.endpoint", () -> minioEndpoint());
        registry.add("kay-ledger.object-storage.bucket", () -> BUCKET);
        registry.add("kay-ledger.object-storage.region", () -> "us-east-1");
        registry.add("kay-ledger.object-storage.access-key", () -> ACCESS_KEY);
        registry.add("kay-ledger.object-storage.secret-key", () -> SECRET_KEY);
        registry.add("kay-ledger.object-storage.path-style-access-enabled", () -> "true");
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    void hardeningSearchRiskReportingAndExportsWorkEndToEnd() {
        bootstrapTenants();
        HttpHeaders ownerHeaders = workspaceHeaders("k12-read", ALPHA, OWNER);
        HttpHeaders betaHeaders = workspaceHeaders("k12-beta-read", BETA, BETA_OWNER);
        Map<String, Object> providerProfile = post("/api/provider-profiles", workspaceHeaders("k12-provider-profile", ALPHA, OWNER), Map.of("actorId", actorId(PROVIDER), "displayName", "K12 Provider"));
        Map<String, Object> customerProfile = post("/api/customer-profiles", workspaceHeaders("k12-customer-profile", ALPHA, OWNER), Map.of("actorId", actorId(CUSTOMER), "displayName", "K12 Customer"));
        String providerProfileId = providerProfile.get("id").toString();
        String customerProfileId = customerProfile.get("id").toString();
        createAccounts(ALPHA, OWNER);

        Map<String, Object> config = post("/api/providers/configs", workspaceHeaders("k12-provider-config", ALPHA, OWNER), Map.of(
                "providerKey", "internal-test",
                "displayName", "Internal Test",
                "signingSecret", SECRET,
                "callbackToken", "k12-callback-token"));
        String callbackToken = config.get("callbackToken").toString();

        String paymentId = createPayment(providerProfileId, customerProfileId, 12_000, "2036-01-07T10:00:00Z", "k12-payment");
        String rawPaymentCallback = """
                {
                  "amountMinor": 12000,
                  "businessReferenceId": "%s",
                  "callbackType": "PAYMENT_SETTLED",
                  "providerSequence": 10,
                  "providerEventId": "evt-payment-settled"
                }
                """.formatted(paymentId);
        int attemptsBeforeDuplicate = countPaymentAttempts(paymentId);
        Map<String, Object> applied = postRawCallback(callbackToken, rawPaymentCallback, SECRET, HttpStatus.OK);
        Map<String, Object> duplicate = postRawCallback(callbackToken, rawPaymentCallback, SECRET, HttpStatus.OK);
        assertThat(applied.get("processingStatus")).isEqualTo("APPLIED");
        assertThat(duplicate.get("processingStatus")).isEqualTo("APPLIED");
        assertThat(countPaymentAttempts(paymentId)).isEqualTo(attemptsBeforeDuplicate + 3);
        postRawCallback(callbackToken, rawPaymentCallback, "wrong-secret", HttpStatus.FORBIDDEN);
        String badReference = UUID.randomUUID().toString();
        postRawCallback(callbackToken, callbackPayload("evt-missing-payment", 20, "PAYMENT_SETTLED", badReference, 12000), SECRET, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(callbackStatus("evt-missing-payment")).isEqualTo("FAILED");

        String refundId = post("/api/payments/refunds/partial", workspaceHeaders("k12-refund", ALPHA, OWNER), Map.of(
                "paymentIntentId", paymentId,
                "amountMinor", 1_000,
                "externalReference", "refund-request-1")).get("id").toString();
        postRawCallback(callbackToken, callbackPayload("evt-refund-succeeded", 30, "REFUND_SUCCEEDED", refundId, 1000), SECRET, HttpStatus.OK);
        assertThat(refundStatus(refundId)).isEqualTo("SUCCEEDED");

        String payoutSourcePayment = createSettledPayment(providerProfileId, customerProfileId, 120_000, "2036-01-07T12:00:00Z", "k12-big-payment");
        String payoutId = post("/api/payments/payouts", workspaceHeaders("k12-payout", ALPHA, OWNER), Map.of(
                "providerProfileId", providerProfileId,
                "currencyCode", "USD",
                "amountMinor", 100_000)).get("id").toString();
        postRawCallback(callbackToken, callbackPayload("evt-payout-succeeded", 40, "PAYOUT_SUCCEEDED", payoutId, 100000), SECRET, HttpStatus.OK);
        assertThat(payoutStatus(payoutId)).isEqualTo("SUCCEEDED");

        String amountMismatchPayment = createSettledPayment(providerProfileId, customerProfileId, 12_000, "2036-01-07T14:00:00Z", "k12-amount-mismatch");
        postRawCallback(callbackToken, callbackPayload("evt-amount-mismatch", 50, "PAYMENT_SETTLED", amountMismatchPayment, 999), SECRET, HttpStatus.OK);
        post("/api/reconciliation/runs", workspaceHeaders("k12-recon-amount", ALPHA, OWNER), Map.of("runType", "FULL"));
        List<?> amountMismatches = getList("/api/reconciliation/mismatches", ownerHeaders, HttpStatus.OK);
        Map<?, ?> amountMismatch = firstMismatch(amountMismatches, "AMOUNT_MISMATCH");
        assertThat(amountMismatch.get("suggestedAction")).isEqualTo("MANUAL_REVIEW");
        assertStatus("/api/reconciliation/mismatches/" + amountMismatch.get("id") + "/apply-repair", HttpMethod.POST, workspaceHeaders("k12-unsafe-repair", ALPHA, OWNER), Map.of("note", "unsafe"), HttpStatus.BAD_REQUEST);
        jdbcTemplate.update("UPDATE payment_intents SET status = 'CREATED' WHERE id = ?::uuid", amountMismatchPayment);
        post("/api/reconciliation/runs", workspaceHeaders("k12-recon-state", ALPHA, OWNER), Map.of("runType", "FULL"));
        List<?> mismatches = getList("/api/reconciliation/mismatches", ownerHeaders, HttpStatus.OK);
        Map<?, ?> stateMismatch = firstMismatch(mismatches, "STATE_MISMATCH", amountMismatchPayment);
        post("/api/reconciliation/mismatches/" + stateMismatch.get("id") + "/apply-repair", workspaceHeaders("k12-safe-repair", ALPHA, OWNER), Map.of("note", "safe provider truth repair"));
        assertThat(paymentStatus(amountMismatchPayment)).isEqualTo("SETTLED");

        assertStatus("/api/messaging/outbox/relay", HttpMethod.POST, betaHeadersWithKey("k12-beta-relay"), Map.of(), HttpStatus.OK);
        String replayDedupeKey = "projection-replay-" + UUID.randomUUID();
        String replayPayload = outboxPayloadForPayment(payoutSourcePayment);
        jdbcTemplate.update("DELETE FROM payment_projection WHERE payment_intent_id = ?::uuid", payoutSourcePayment);
        jdbcTemplate.update("""
                INSERT INTO inbox_messages (workspace_id, topic, partition_id, message_key, event_id, dedupe_key, consumer_name, outcome, payload_json, retry_count, parked_at)
                VALUES (?::uuid, 'kay-ledger.kay012.test.events', 0, 'projection-replay', ?::uuid, ?, 'projection-consumer', 'PARKED', ?::jsonb, 2, now())
                """, workspaceId(ALPHA).toString(), UUID.randomUUID().toString(), replayDedupeKey, replayPayload);
        assertStatus("/api/messaging/inbox/parked/projection-consumer/" + replayDedupeKey, HttpMethod.GET, betaHeaders, null, HttpStatus.NOT_FOUND);
        post("/api/messaging/inbox/parked/projection-consumer/" + replayDedupeKey + "/replay", workspaceHeaders("k12-replay", ALPHA, OWNER), Map.of());
        assertThat(projectionStatus(payoutSourcePayment)).isEqualTo("SETTLED");

        post("/api/investigation/reindex", workspaceHeaders("k12-reindex", ALPHA, OWNER), Map.of());
        assertThat(getList("/api/investigation/search?paymentId=" + paymentId, ownerHeaders, HttpStatus.OK)).isNotEmpty();
        assertThat(getList("/api/investigation/provider-events/evt-payment-settled", ownerHeaders, HttpStatus.OK)).isNotEmpty();

        triggerRiskRules(providerProfileId, customerProfileId);
        List<?> riskFlags = getList("/api/risk/flags", ownerHeaders, HttpStatus.OK);
        assertThat(ruleCodes(riskFlags)).contains("REPEATED_FAILED_PAYMENT_BURST", "REFUND_VELOCITY_SPIKE", "SUSPICIOUS_PAYOUT_THRESHOLD");
        List<?> reviews = getList("/api/risk/reviews", ownerHeaders, HttpStatus.OK);
        assertThat(reviews).hasSizeGreaterThanOrEqualTo(3);
        Map<?, ?> reviewForBlock = reviewForRule(reviews, riskFlags, "REPEATED_FAILED_PAYMENT_BURST");
        Map<?, ?> firstReview = (Map<?, ?>) reviews.get(0);
        Map<?, ?> secondReview = (Map<?, ?>) reviews.get(1);
        post("/api/risk/reviews/" + firstReview.get("id") + "/in-review", workspaceHeaders("k12-review-in-review", ALPHA, OWNER), Map.of());
        post("/api/risk/reviews/" + firstReview.get("id") + "/decisions", workspaceHeaders("k12-review-decision", ALPHA, OWNER), Map.of("outcome", "REVIEW", "reason", "operator review continues"));
        post("/api/risk/reviews/" + secondReview.get("id") + "/decisions", workspaceHeaders("k12-allow-decision", ALPHA, OWNER), Map.of("outcome", "ALLOW", "reason", "known provider behavior"));
        post("/api/risk/reviews/" + reviewForBlock.get("id") + "/decisions", workspaceHeaders("k12-block-decision", ALPHA, OWNER), Map.of("outcome", "BLOCK", "reason", "provider paused"));
        assertThat(getList("/api/risk/decisions", ownerHeaders, HttpStatus.OK).stream()
                .map(Map.class::cast)
                .map(decision -> decision.get("outcome").toString())
                .toList()).contains("ALLOW", "REVIEW", "BLOCK");
        assertStatus("/api/payments/payouts", HttpMethod.POST, workspaceHeaders("k12-blocked-payout", ALPHA, OWNER), Map.of(
                "providerProfileId", providerProfileId,
                "currencyCode", "USD",
                "amountMinor", 100), HttpStatus.BAD_REQUEST);

        List<?> summaries = postList("/api/reporting/summaries/providers/refresh", workspaceHeaders("k12-summary-refresh", ALPHA, OWNER), Map.of());
        assertThat(summaries).anySatisfy(summary -> assertThat(((Number) ((Map<?, ?>) summary).get("settledGrossAmountMinor")).longValue()).isGreaterThan(0));
        Map<String, Object> exportJob = post("/api/reporting/exports", workspaceHeaders("k12-export", ALPHA, OWNER), Map.of("exportType", "PROVIDER_STATEMENT"));
        assertThat(exportJob.get("status")).isEqualTo("SUCCEEDED");
        List<?> exportJobs = getList("/api/reporting/exports/jobs", ownerHeaders, HttpStatus.OK);
        List<?> artifacts = getList("/api/reporting/exports/artifacts", ownerHeaders, HttpStatus.OK);
        assertThat(exportJobs).isNotEmpty();
        assertThat(artifacts).isNotEmpty();
        String storageKey = ((Map<?, ?>) artifacts.get(0)).get("storageKey").toString();
        assertObjectExists(storageKey);

        assertThat(getList("/api/providers/callbacks", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/investigation/search?paymentId=" + paymentId, betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/risk/flags", betaHeaders, HttpStatus.OK)).isEmpty();
        assertThat(getList("/api/reporting/summaries/providers", betaHeaders, HttpStatus.OK)).isEmpty();
    }

    private void bootstrapTenants() {
        post("/api/workspaces", headers("k12-ws-alpha"), Map.of("slug", ALPHA, "displayName", "K12 Alpha"));
        post("/api/workspaces", headers("k12-ws-beta"), Map.of("slug", BETA, "displayName", "K12 Beta"));
        post("/api/actors", headers("k12-owner"), actor(OWNER, "K12 Owner"));
        post("/api/actors", headers("k12-provider"), actor(PROVIDER, "K12 Provider"));
        post("/api/actors", headers("k12-customer"), actor(CUSTOMER, "K12 Customer"));
        post("/api/actors", headers("k12-beta-owner"), actor(BETA_OWNER, "K12 Beta Owner"));
        post("/api/memberships", headers("k12-membership-owner"), Map.of("workspaceSlug", ALPHA, "actorId", actorId(OWNER), "role", "OWNER"));
        post("/api/memberships", workspaceHeaders("k12-membership-provider", ALPHA, OWNER), Map.of("workspaceSlug", ALPHA, "actorId", actorId(PROVIDER), "role", "PROVIDER"));
        post("/api/memberships", workspaceHeaders("k12-membership-customer", ALPHA, OWNER), Map.of("workspaceSlug", ALPHA, "actorId", actorId(CUSTOMER), "role", "CUSTOMER"));
        post("/api/memberships", headers("k12-membership-beta"), Map.of("workspaceSlug", BETA, "actorId", actorId(BETA_OWNER), "role", "OWNER"));
    }

    private void createAccounts(String workspaceSlug, String ownerKey) {
        post("/api/finance/accounts", workspaceHeaders("acct-auth-" + workspaceSlug, workspaceSlug, ownerKey), account("1110", "Authorized Funds", "ASSET", "AUTHORIZED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-platform-" + workspaceSlug, workspaceSlug, ownerKey), account("1120", "Platform Clearing", "ASSET", "PLATFORM_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("acct-captured-" + workspaceSlug, workspaceSlug, ownerKey), account("1130", "Captured Funds", "ASSET", "CAPTURED_FUNDS"));
        post("/api/finance/accounts", workspaceHeaders("acct-seller-" + workspaceSlug, workspaceSlug, ownerKey), account("2100", "Seller Payable", "LIABILITY", "SELLER_PAYABLE"));
        post("/api/finance/accounts", workspaceHeaders("acct-fee-" + workspaceSlug, workspaceSlug, ownerKey), account("4100", "Fee Revenue", "REVENUE", "FEE_REVENUE"));
        post("/api/finance/accounts", workspaceHeaders("acct-cash-" + workspaceSlug, workspaceSlug, ownerKey), account("1000", "Cash Placeholder", "ASSET", "CASH_PLACEHOLDER"));
        post("/api/finance/accounts", workspaceHeaders("acct-payout-" + workspaceSlug, workspaceSlug, ownerKey), account("2200", "Payout Clearing", "LIABILITY", "PAYOUT_CLEARING"));
        post("/api/finance/accounts", workspaceHeaders("acct-refund-reserve-" + workspaceSlug, workspaceSlug, ownerKey), account("1140", "Refund Reserve", "ASSET", "REFUND_RESERVE"));
        post("/api/finance/accounts", workspaceHeaders("acct-refund-liability-" + workspaceSlug, workspaceSlug, ownerKey), account("2300", "Refund Liability", "LIABILITY", "REFUND_LIABILITY"));
        post("/api/finance/accounts", workspaceHeaders("acct-dispute-" + workspaceSlug, workspaceSlug, ownerKey), account("1150", "Dispute Reserve", "ASSET", "DISPUTE_RESERVE"));
        post("/api/finance/accounts", workspaceHeaders("acct-frozen-" + workspaceSlug, workspaceSlug, ownerKey), account("2400", "Frozen Payable", "LIABILITY", "FROZEN_PAYABLE"));
    }

    private String createPayment(String providerProfileId, String customerProfileId, int amountMinor, String start, String keyPrefix) {
        Map<String, Object> offering = post("/api/offerings", workspaceHeaders(keyPrefix + "-offering", ALPHA, OWNER), scheduledOffering(providerProfileId, amountMinor, keyPrefix));
        String offeringId = ((Map<?, ?>) offering.get("offering")).get("id").toString();
        post("/api/offerings/" + offeringId + "/publish", workspaceHeaders(keyPrefix + "-publish", ALPHA, OWNER), Map.of());
        post("/api/finance/fee-rules", workspaceHeaders(keyPrefix + "-fee", ALPHA, OWNER), Map.of(
                "offeringId", offeringId,
                "ruleType", "BASIS_POINTS",
                "basisPoints", 1000,
                "currencyCode", "USD"));
        Map<String, Object> booking = post("/api/bookings", workspaceHeaders(keyPrefix + "-booking", ALPHA, CUSTOMER), scheduledBooking(offeringId, customerProfileId, start));
        String bookingId = ((Map<?, ?>) booking.get("booking")).get("id").toString();
        Map<String, Object> intent = post("/api/payments/intents", workspaceHeaders(keyPrefix + "-intent", ALPHA, OWNER), Map.of("bookingId", bookingId, "externalReference", keyPrefix + "-external"));
        return ((Map<?, ?>) intent.get("paymentIntent")).get("id").toString();
    }

    private String createSettledPayment(String providerProfileId, String customerProfileId, int amountMinor, String start, String keyPrefix) {
        String paymentId = createPayment(providerProfileId, customerProfileId, amountMinor, start, keyPrefix);
        post("/api/payments/intents/" + paymentId + "/authorize", workspaceHeaders(keyPrefix + "-authorize", ALPHA, OWNER), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentId + "/capture", workspaceHeaders(keyPrefix + "-capture", ALPHA, OWNER), Map.of("amountMinor", amountMinor));
        post("/api/payments/intents/" + paymentId + "/settle", workspaceHeaders(keyPrefix + "-settle", ALPHA, OWNER), Map.of("amountMinor", amountMinor));
        return paymentId;
    }

    private void triggerRiskRules(String providerProfileId, String customerProfileId) {
        for (int i = 0; i < 3; i++) {
            String paymentId = createPayment(providerProfileId, customerProfileId, 12_000, "2036-01-08T1" + i + ":00:00Z", "k12-failed-" + i);
            post("/api/payments/intents/" + paymentId + "/fail", workspaceHeaders("k12-fail-" + i, ALPHA, OWNER), Map.of("externalReference", "risk-fail-" + i));
        }
        String refundSource = createSettledPayment(providerProfileId, customerProfileId, 12_000, "2036-01-08T14:00:00Z", "k12-refund-risk-source");
        for (int i = 0; i < 3; i++) {
            post("/api/payments/refunds/partial", workspaceHeaders("k12-refund-risk-" + i, ALPHA, OWNER), Map.of(
                    "paymentIntentId", refundSource,
                    "amountMinor", 1_000,
                    "externalReference", "risk-refund-" + i));
        }
    }

    private Map<String, Object> postRawCallback(String callbackToken, String rawPayload, String secret, HttpStatus expectedStatus) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Provider-Signature", sign(secret, rawPayload));
        ResponseEntity<Map> response = exchange("/api/providers/callbacks/" + callbackToken, HttpMethod.POST, headers, rawPayload, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody() == null ? Map.of() : asMap(response.getBody());
    }

    private String callbackPayload(String eventId, long sequence, String type, String referenceId, long amountMinor) {
        return "{\"providerEventId\":\"" + eventId
                + "\",\"providerSequence\":" + sequence
                + ",\"callbackType\":\"" + type
                + "\",\"businessReferenceId\":\"" + referenceId
                + "\",\"amountMinor\":" + amountMinor + "}";
    }

    private Map<?, ?> firstMismatch(List<?> mismatches, String category) {
        return mismatches.stream()
                .map(Map.class::cast)
                .filter(mismatch -> category.equals(mismatch.get("driftCategory")))
                .findFirst()
                .orElseThrow();
    }

    private Map<?, ?> firstMismatch(List<?> mismatches, String category, String referenceId) {
        return mismatches.stream()
                .map(Map.class::cast)
                .filter(mismatch -> category.equals(mismatch.get("driftCategory")))
                .filter(mismatch -> referenceId.equals(mismatch.get("businessReferenceId").toString()))
                .findFirst()
                .orElseThrow();
    }

    private List<String> ruleCodes(List<?> riskFlags) {
        return riskFlags.stream()
                .map(Map.class::cast)
                .map(flag -> flag.get("ruleCode").toString())
                .toList();
    }

    private Map<?, ?> reviewForRule(List<?> reviews, List<?> riskFlags, String ruleCode) {
        String flagId = riskFlags.stream()
                .map(Map.class::cast)
                .filter(flag -> ruleCode.equals(flag.get("ruleCode")))
                .findFirst()
                .orElseThrow()
                .get("id")
                .toString();
        return reviews.stream()
                .map(Map.class::cast)
                .filter(review -> flagId.equals(review.get("riskFlagId").toString()))
                .findFirst()
                .orElseThrow();
    }

    private String outboxPayloadForPayment(String paymentId) {
        return jdbcTemplate.queryForObject("""
                SELECT payload_json::text
                FROM outbox_events
                WHERE aggregate_id = ?::uuid
                ORDER BY created_at DESC
                LIMIT 1
                """, String.class, paymentId);
    }

    private void assertObjectExists(String storageKey) {
        try (S3Client client = S3Client.builder()
                .endpointOverride(URI.create(minioEndpoint()))
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build()) {
            assertThat(client.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(storageKey).build()).contentLength()).isGreaterThan(0);
        }
    }

    private String callbackStatus(String eventId) {
        return jdbcTemplate.queryForObject("SELECT processing_status FROM provider_callbacks WHERE provider_event_id = ?", String.class, eventId);
    }

    private int countPaymentAttempts(String paymentId) {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM payment_attempts WHERE payment_intent_id = ?::uuid", Integer.class, paymentId);
    }

    private String refundStatus(String refundId) {
        return jdbcTemplate.queryForObject("SELECT status FROM refunds WHERE id = ?::uuid", String.class, refundId);
    }

    private String payoutStatus(String payoutId) {
        return jdbcTemplate.queryForObject("SELECT status FROM payout_requests WHERE id = ?::uuid", String.class, payoutId);
    }

    private String paymentStatus(String paymentId) {
        return jdbcTemplate.queryForObject("SELECT status FROM payment_intents WHERE id = ?::uuid", String.class, paymentId);
    }

    private String projectionStatus(String paymentId) {
        return jdbcTemplate.queryForObject("SELECT latest_payment_status FROM payment_projection WHERE payment_intent_id = ?::uuid", String.class, paymentId);
    }

    private UUID workspaceId(String slug) {
        return jdbcTemplate.queryForObject("SELECT id FROM workspaces WHERE slug = ?", UUID.class, slug);
    }

    private UUID actorId(String actorKey) {
        return jdbcTemplate.queryForObject("SELECT id FROM actors WHERE actor_key = ?", UUID.class, actorKey);
    }

    private Map<String, Object> actor(String actorKey, String displayName) {
        return Map.of("actorKey", actorKey, "displayName", displayName);
    }

    private Map<String, Object> account(String code, String name, String type, String purpose) {
        return Map.of("accountCode", code, "accountName", name, "accountType", type, "accountPurpose", purpose, "currencyCode", "USD");
    }

    private Map<String, Object> scheduledOffering(String providerProfileId, int amountMinor, String keyPrefix) {
        return Map.ofEntries(
                Map.entry("providerProfileId", providerProfileId),
                Map.entry("title", "K12 Session " + keyPrefix),
                Map.entry("offerType", "SCHEDULED_TIME"),
                Map.entry("pricingMetadata", Map.of("display", "fixed")),
                Map.entry("durationMinutes", 60),
                Map.entry("minNoticeMinutes", 0),
                Map.entry("maxNoticeDays", 5000),
                Map.entry("slotIntervalMinutes", 30),
                Map.entry("schedulingMetadata", Map.of("boundary", "k12")),
                Map.entry("pricingRules", List.of(Map.of("ruleType", "FIXED_PRICE", "currencyCode", "USD", "amountMinor", amountMinor, "sortOrder", 0))),
                Map.entry("availabilityWindows", List.of(
                        Map.of("weekday", 1, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 2, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 3, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 4, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 5, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 6, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"),
                        Map.of("weekday", 7, "startLocalTime", "00:00:00", "endLocalTime", "23:59:00"))));
    }

    private Map<String, Object> scheduledBooking(String offeringId, String customerProfileId, String start) {
        return Map.of(
                "offeringId", offeringId,
                "customerProfileId", customerProfileId,
                "scheduledStartAt", Instant.parse(start).toString(),
                "scheduledEndAt", Instant.parse(start).plusSeconds(3600).toString(),
                "holdTtlSeconds", 900);
    }

    private Map<String, Object> post(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<Map> response = exchange(path, HttpMethod.POST, headers, body, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody() == null ? Map.of() : asMap(response.getBody());
    }

    private List<?> postList(String path, HttpHeaders headers, Map<String, Object> body) {
        ResponseEntity<List> response = exchange(path, HttpMethod.POST, headers, body, List.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private List<?> getList(String path, HttpHeaders headers, HttpStatus expectedStatus) {
        ResponseEntity<List> response = exchange(path, HttpMethod.GET, withoutIdempotency(headers), null, List.class);
        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        return response.getBody() == null ? List.of() : response.getBody();
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

    private HttpHeaders betaHeadersWithKey(String idempotencyKey) {
        return workspaceHeaders(idempotencyKey, BETA, BETA_OWNER);
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

    private static String sign(String secret, String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static String minioEndpoint() {
        return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
    }
}
