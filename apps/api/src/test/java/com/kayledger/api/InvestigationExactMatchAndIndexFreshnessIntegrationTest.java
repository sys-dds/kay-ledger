package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.investigation.application.InvestigationSearchService;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class InvestigationExactMatchAndIndexFreshnessIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_investigation_test")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.investigation.test.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-investigation-exact-" + System.nanoTime());
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    InvestigationIndexingService indexingService;

    @Autowired
    InvestigationSearchService searchService;

    @Test
    void invariant_exact_identifier_search_and_incremental_freshness_do_not_depend_on_workspace_reindex() {
        Fixture fixture = fixture("investigation-alpha");
        indexingService.indexReference(fixture.workspaceId(), "PAYMENT_INTENT", fixture.paymentId());
        indexingService.indexReference(fixture.workspaceId(), "PROVIDER_CALLBACK", fixture.callbackId());
        indexingService.indexReference(fixture.workspaceId(), "PAYMENT_INTENT", fixture.paymentId());

        assertThat(searchService.search(context(fixture), new InvestigationSearchService.SearchCommand(
                fixture.paymentId().toString(), null, null, null, null, null, null, null, null, null))).isNotEmpty();
        assertThat(searchService.byProviderEvent(context(fixture), "evt-investigation-exact")).isNotEmpty();
        assertThat(searchService.byExternalReference(context(fixture), "external-investigation-exact")).isNotEmpty();
        assertThat(indexStateRows(fixture.paymentId())).isEqualTo(1);

        UUID laterCallbackId = UUID.randomUUID();
        insertCallback(fixture.workspaceId(), fixture.providerConfigId(), laterCallbackId, "evt-investigation-incremental", fixture.paymentId());
        indexingService.indexReference(fixture.workspaceId(), "PROVIDER_CALLBACK", laterCallbackId);

        assertThat(searchService.byProviderEvent(context(fixture), "evt-investigation-incremental")).isNotEmpty();
        assertThat(searchService.reindex(context(fixture)).failed()).isZero();
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
        UUID callbackId = UUID.randomUUID();

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
                VALUES (?, ?, ?, 'Investigation Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId, timestamp("2036-01-01T10:00:00Z"), timestamp("2036-01-01T11:00:00Z"), timestamp("2036-01-01T09:00:00Z"));
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', 10000, 1000, 9000, 10000, 10000, 10000, 'external-investigation-exact')
                """, paymentId, workspaceId, bookingId, providerProfileId);
        jdbcTemplate.update("INSERT INTO provider_configs (id, workspace_id, provider_key, display_name, signing_secret, callback_token) VALUES (?, ?, 'exact-provider', 'Exact Provider', 'secret', ?)", providerConfigId, workspaceId, "token-" + providerConfigId);
        insertCallback(workspaceId, providerConfigId, callbackId, "evt-investigation-exact", paymentId);
        return new Fixture(workspaceId, slug, ownerId, membershipId, paymentId, providerConfigId, callbackId);
    }

    private void insertCallback(UUID workspaceId, UUID providerConfigId, UUID callbackId, String eventId, UUID paymentId) {
        jdbcTemplate.update("""
                INSERT INTO provider_callbacks (
                    id, workspace_id, provider_config_id, provider_key, provider_event_id, provider_sequence,
                    callback_type, business_reference_type, business_reference_id, payload_json,
                    signature_header, signature_verified, dedupe_key, processing_status, applied_at
                )
                VALUES (?, ?, ?, 'exact-provider', ?, 1, 'PAYMENT_SETTLED', 'PAYMENT_INTENT', ?, '{}'::jsonb, 'sig', true, ?, 'APPLIED', now())
                """, callbackId, workspaceId, providerConfigId, eventId, paymentId, "exact-provider:" + eventId);
    }

    private int indexStateRows(UUID paymentId) {
        Integer value = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM operator_search_index_state
                WHERE reference_id = ?
                """, Integer.class, paymentId);
        return value == null ? 0 : value;
    }

    private AccessContext context(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private static Timestamp timestamp(String value) {
        return Timestamp.from(Instant.parse(value));
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID paymentId, UUID providerConfigId, UUID callbackId) {
    }
}
