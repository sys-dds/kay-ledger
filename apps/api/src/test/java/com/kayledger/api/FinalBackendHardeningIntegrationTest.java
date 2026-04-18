package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
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
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.investigation.application.InvestigationSearchService;
import com.kayledger.api.provider.application.ProviderCallbackService;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.api.InternalFailureException;
import com.kayledger.api.temporal.application.OperatorWorkflowService;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.object-storage.endpoint=http://127.0.0.1:1",
        "kay-ledger.object-storage.bucket=kay-ledger-kay016-failure",
        "management.endpoints.web.exposure.include=health,metrics"
})
class FinalBackendHardeningIntegrationTest {

    private static final String SECRET = "kay016-secret";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay016_final")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay016.final.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-kay016-final-" + System.nanoTime());
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ProviderCallbackService providerCallbackService;

    @Autowired
    InvestigationSearchService investigationSearchService;

    @Autowired
    InvestigationIndexingService investigationIndexingService;

    @Autowired
    ReportingService reportingService;

    @Autowired
    OperatorWorkflowService operatorWorkflowService;

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void invariant_degraded_modes_are_truthful_and_metrics_are_exposed_without_fake_controls() {
        Fixture fixture = fixture("kay016-final");
        String canonicalOnlyPayload = rawPayload("evt-kay016-canonical-rejected", fixture.paymentId());
        assertThatThrownBy(() -> providerCallbackService.ingestWorkspaceScoped(
                paymentContext(fixture),
                "kay016-provider",
                sign(canonicalPayload("evt-kay016-canonical-rejected", fixture.paymentId())),
                canonicalOnlyPayload.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(ForbiddenException.class);

        String rawPayload = rawPayload("evt-kay016-raw-accepted", fixture.paymentId());
        providerCallbackService.ingestWorkspaceScoped(
                paymentContext(fixture),
                "kay016-provider",
                sign(rawPayload),
                rawPayload.getBytes(StandardCharsets.UTF_8));
        assertThat(investigationSearchService.byProviderEvent(paymentContext(fixture), "evt-kay016-raw-accepted")).isNotEmpty();
        investigationIndexingService.indexReference(fixture.workspaceId(), "PAYMENT_INTENT", fixture.paymentId());
        assertThat(investigationSearchService.byProviderEvent(paymentContext(fixture), "evt-kay016-raw-accepted")).isNotEmpty();

        assertThatThrownBy(() -> reportingService.generateSynchronousExport(financeContext(fixture), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT")))
                .isInstanceOf(InternalFailureException.class)
                .hasMessageContaining("Export generation or artifact storage failed");
        String exportStatus = jdbcTemplate.queryForObject("""
                SELECT status FROM export_jobs
                WHERE workspace_id = ?
                ORDER BY requested_at DESC
                LIMIT 1
                """, String.class, fixture.workspaceId());
        assertThat(exportStatus).isEqualTo("FAILED");

        var workflow = operatorWorkflowService.createRequested(
                fixture.workspaceId(),
                OperatorWorkflowService.EXPORT,
                OperatorWorkflowService.EXPORT_JOB,
                UUID.randomUUID(),
                OperatorWorkflowService.API,
                fixture.ownerId(),
                1,
                "Metrics proof requested.");
        operatorWorkflowService.markRunning(fixture.workspaceId(), workflow.workflowId(), "Metrics proof running.");
        operatorWorkflowService.markSucceeded(fixture.workspaceId(), workflow.workflowId(), "Metrics proof completed.");
        String metricsBody = restTemplate.getForObject("/actuator/metrics/kayledger.operator_workflows.outcomes", String.class);
        assertThat(metricsBody).contains("kayledger.operator_workflows.outcomes");
        assertThat(java.util.Arrays.stream(com.kayledger.api.temporal.application.OperatorWorkflowQueryService.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName))
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
                VALUES (?, ?, ?, 'KAY016 Final Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId, timestamp("2036-04-01T10:00:00Z"), timestamp("2036-04-01T11:00:00Z"), timestamp("2036-04-01T09:00:00Z"));
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'CREATED', 'USD', 10000, 1000, 9000, 0, 0, 0, 'kay016-final')
                """, paymentId, workspaceId, bookingId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO provider_configs (id, workspace_id, provider_key, display_name, signing_secret, callback_token)
                VALUES (?, ?, 'kay016-provider', 'KAY016 Provider', ?, ?)
                """, providerConfigId, workspaceId, SECRET, "token-" + providerConfigId);
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId, paymentId);
    }

    private AccessContext paymentContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private AccessContext financeContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE"), Set.of());
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
