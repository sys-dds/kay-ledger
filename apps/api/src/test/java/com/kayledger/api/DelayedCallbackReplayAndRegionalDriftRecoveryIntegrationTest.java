package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.provider.application.ProviderCallbackService;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionFaultService.FaultCommand;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService.RecoveryCommand;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-a",
        "kay-ledger.region.peer-region-ids=region-b",
        "kay-ledger.region.replication-consumer-enabled=false",
        "kay-ledger.search.opensearch.endpoint=http://localhost:1",
        "kay-ledger.object-storage.endpoint=http://localhost:1"
})
class DelayedCallbackReplayAndRegionalDriftRecoveryIntegrationTest {

    private static final String SECRET = "kay019-secret";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay019_callbacks")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay019.callbacks.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay019.callbacks.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ProviderCallbackService providerCallbackService;

    @Autowired
    RegionFaultService regionFaultService;

    @Autowired
    RegionalRecoveryService regionalRecoveryService;

    @Test
    void invariant_delayed_callback_is_durable_redrivable_and_resolves_recorded_drift() {
        Fixture fixture = fixture("kay019-callback");
        AccessContext context = paymentWrite(fixture);
        regionFaultService.inject(context, new FaultCommand(RegionFaultService.DELAY_PROVIDER_CALLBACK_APPLY, "WORKSPACE", Map.of(), "delay callback"));

        String payload = payload("evt-kay019-delayed", 10, fixture.paymentId());
        var delayed = providerCallbackService.ingestWorkspaceScoped(context, "kay019-provider", sign(payload), payload.getBytes(StandardCharsets.UTF_8));
        assertThat(delayed.processingStatus()).isEqualTo("DELAYED_BY_DRILL");
        assertThat(jdbcTemplate.queryForObject("SELECT status FROM payment_intents WHERE id = ?", String.class, fixture.paymentId()))
                .isEqualTo("CREATED");

        var drifts = regionalRecoveryService.scan(context);
        var drift = drifts.stream()
                .filter(record -> RegionalRecoveryService.DELAYED_PROVIDER_CALLBACK_BACKLOG.equals(record.driftType()))
                .findFirst()
                .orElseThrow();
        var action = regionalRecoveryService.requestRecovery(context, new RecoveryCommand(
                drift.id(),
                RegionalRecoveryService.REDRIVE_DELAYED_PROVIDER_CALLBACK,
                "PROVIDER_CALLBACK",
                delayed.id().toString()));

        assertThat(action.status()).isEqualTo("SUCCEEDED");
        assertThat(providerCallbackService.listCallbacks(context)).anySatisfy(callback -> {
            assertThat(callback.id()).isEqualTo(delayed.id());
            assertThat(callback.processingStatus()).isEqualTo("APPLIED");
        });
        assertThat(jdbcTemplate.queryForObject("SELECT status FROM payment_intents WHERE id = ?", String.class, fixture.paymentId()))
                .isEqualTo("FAILED");
        assertThat(regionalRecoveryService.listUnresolvedDrift(context)).noneSatisfy(record ->
                assertThat(record.id()).isEqualTo(drift.id()));
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
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", workspaceId);
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
                VALUES (?, ?, ?, 'KAY019 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId,
                java.sql.Timestamp.from(Instant.parse("2036-05-01T10:00:00Z")),
                java.sql.Timestamp.from(Instant.parse("2036-05-01T11:00:00Z")),
                java.sql.Timestamp.from(Instant.parse("2036-05-01T09:00:00Z")));
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'CREATED', 'USD', 10000, 1000, 9000, 0, 0, 0, 'kay019-callback')
                """, paymentId, workspaceId, bookingId, providerProfileId);
        providerCallbackService.createConfig(paymentWrite(new Fixture(workspaceId, slug, ownerId, membershipId, paymentId)),
                new ProviderCallbackService.CreateProviderConfigCommand("kay019-provider", "KAY019 Provider", SECRET, "token-" + workspaceId));
        return new Fixture(workspaceId, slug, ownerId, membershipId, paymentId);
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private static String payload(String eventId, long sequence, UUID paymentId) {
        return """
                {"providerEventId":"%s","providerSequence":%d,"callbackType":"PAYMENT_FAILED","businessReferenceId":"%s","amountMinor":0,"metadata":null}
                """.formatted(eventId, sequence, paymentId).trim();
    }

    private static String sign(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID paymentId) {
    }
}
