package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.investigation.application.InvestigationSearchService;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.provider.application.ProviderCallbackService;
import com.kayledger.api.risk.application.RiskService;
import com.kayledger.api.risk.store.RiskStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.temporal.application.OperatorWorkflowQueryService;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false"
})
class Kay015FinalTruthfulnessHardeningIntegrationTest {

    private static final String SECRET = "kay015-secret";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay015_truth")
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

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay015.truth.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-kay015-truth-" + System.nanoTime());
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ProviderCallbackService providerCallbackService;

    @Autowired
    InvestigationSearchService investigationSearchService;

    @Autowired
    RiskStore riskStore;

    @Autowired
    RiskService riskService;

    @Autowired
    PaymentService paymentService;

    @Test
    void invariant_final_truthfulness_hardening_uses_real_paths_and_hides_unsupported_retry() {
        Fixture fixture = fixture("kay015-truth-alpha");
        String canonicalOnlyPayload = rawPayload("evt-kay015-canonical-rejected", fixture.paymentId());
        assertThatThrownBy(() -> providerCallbackService.ingestWorkspaceScoped(
                paymentContext(fixture),
                "kay015-provider",
                sign(canonicalPayload("evt-kay015-canonical-rejected", fixture.paymentId())),
                canonicalOnlyPayload.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(ForbiddenException.class);

        String rawPayload = rawPayload("evt-kay015-raw-accepted", fixture.paymentId());
        providerCallbackService.ingestWorkspaceScoped(
                paymentContext(fixture),
                "kay015-provider",
                sign(rawPayload),
                rawPayload.getBytes(StandardCharsets.UTF_8));

        assertThat(investigationSearchService.byProviderEvent(paymentContext(fixture), "evt-kay015-raw-accepted")).isNotEmpty();

        var flag = riskStore.upsertFlag(fixture.workspaceId(), "PROVIDER_PROFILE", fixture.providerProfileId(), "KAY015_ENFORCEMENT", "HIGH", "truth test", 1);
        var review = riskStore.ensureReview(fixture.workspaceId(), flag.id());
        riskService.decide(paymentContext(fixture), review.id(), new RiskService.DecisionCommand("BLOCK", "hold payouts"));

        assertThat(riskService.enforcementScope(paymentContext(fixture)).blockedMutationScopes())
                .containsEntry("PROVIDER_PROFILE", java.util.List.of(RiskService.MUTATION_PAYOUT_REQUEST, RiskService.MUTATION_PAYOUT_RETRY, RiskService.MUTATION_PAYOUT_OPERATOR_SUCCEED));
        assertThatThrownBy(() -> paymentService.requestPayout(paymentContext(fixture), new PaymentService.PayoutRequestCommand(fixture.providerProfileId(), "USD", 100L)))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("PROVIDER_PROFILE is blocked");
        assertThatThrownBy(() -> riskService.decide(paymentContext(fixture), review.id(), new RiskService.DecisionCommand("BLOCK", "repeat block")))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("transition");
        riskService.decide(paymentContext(fixture), review.id(), new RiskService.DecisionCommand("ALLOW", "cleared"));

        assertThat(Arrays.stream(OperatorWorkflowQueryService.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName))
                .doesNotContain("retry", "restart", "cancel", "terminate");
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        UUID providerActorId = UUID.randomUUID();
        UUID providerMembershipId = UUID.randomUUID();
        UUID customerActorId = UUID.randomUUID();
        UUID customerMembershipId = UUID.randomUUID();
        UUID providerProfileId = UUID.randomUUID();
        UUID customerProfileId = UUID.randomUUID();
        UUID offeringId = UUID.randomUUID();
        UUID bookingId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        UUID providerConfigId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", providerActorId, slug + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", customerActorId, slug + "-customer", "Customer");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", providerMembershipId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'CUSTOMER')", customerMembershipId, workspaceId, customerActorId);
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", providerProfileId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO customer_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Customer')", customerProfileId, workspaceId, customerActorId);
        jdbcTemplate.update("""
                INSERT INTO offerings (id, workspace_id, provider_profile_id, title, status, duration_minutes, offer_type, min_notice_minutes, max_notice_days, slot_interval_minutes)
                VALUES (?, ?, ?, 'KAY015 Truth Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId, timestamp("2036-02-01T10:00:00Z"), timestamp("2036-02-01T11:00:00Z"), timestamp("2036-02-01T09:00:00Z"));
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'CREATED', 'USD', 10000, 1000, 9000, 0, 0, 0, 'kay015-provider-ingress')
                """, paymentId, workspaceId, bookingId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO provider_configs (id, workspace_id, provider_key, display_name, signing_secret, callback_token)
                VALUES (?, ?, 'kay015-provider', 'KAY015 Provider', ?, ?)
                """, providerConfigId, workspaceId, SECRET, "token-" + providerConfigId);
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId, paymentId);
    }

    private AccessContext paymentContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private static String rawPayload(String eventId, UUID paymentId) {
        return """
                {"callbackType":"PAYMENT_FAILED","amountMinor":0,"metadata":null,"providerSequence":1,"businessReferenceId":"%s","providerEventId":"%s"}
                """.formatted(paymentId, eventId).trim();
    }

    private static String canonicalPayload(String eventId, UUID paymentId) {
        return "{\"providerEventId\":\"" + eventId
                + "\",\"providerSequence\":1,\"callbackType\":\"PAYMENT_FAILED\",\"businessReferenceId\":\"" + paymentId
                + "\",\"amountMinor\":0,\"metadata\":null}";
    }

    private static String sign(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return java.util.HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static Timestamp timestamp(String value) {
        return Timestamp.from(Instant.parse(value));
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerProfileId, UUID paymentId) {
    }
}
