package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.provider.store.ProviderStore;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionFaultService.FaultCommand;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionReplicationService.RegionReplicationEvent;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-b",
        "kay-ledger.region.peer-region-ids=region-a",
        "kay-ledger.region.replication-consumer-enabled=true",
        "kay-ledger.search.opensearch.endpoint=http://localhost:1",
        "kay-ledger.object-storage.endpoint=http://localhost:1"
})
class ProviderAndReplicationChaosIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay018_chaos")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay018.chaos.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay018.region.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    ProviderStore providerStore;

    @Autowired
    RegionFaultService regionFaultService;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Test
    void invariant_provider_fault_visibility_and_replication_duplicate_out_of_order_safety_are_truthful() throws Exception {
        Fixture fixture = fixture("kay018-chaos");
        AccessContext context = paymentWrite(fixture);
        regionFaultService.inject(context, new FaultCommand(RegionFaultService.DUPLICATE_PROVIDER_CALLBACK_APPLY, "WORKSPACE", Map.of(), "duplicate drill"));
        regionFaultService.inject(context, new FaultCommand(RegionFaultService.OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY, "WORKSPACE", Map.of(), "out of order drill"));

        assertThat(regionFaultService.active(context))
                .extracting("faultType")
                .contains(RegionFaultService.DUPLICATE_PROVIDER_CALLBACK_APPLY, RegionFaultService.OUT_OF_ORDER_PROVIDER_CALLBACK_APPLY);

        var config = providerStore.createConfig(fixture.workspaceId(), "stripe", "Stripe", "secret", "token-" + fixture.slug());
        UUID callbackBusinessId = UUID.randomUUID();
        String dedupe = "evt-duplicate";
        assertThat(providerStore.insertCallback(fixture.workspaceId(), config.id(), "stripe", "evt-duplicate", 10L, "PAYMENT_SUCCEEDED", "PAYMENT_INTENT", callbackBusinessId, "{}", "sig", true, dedupe)).isPresent();
        assertThat(providerStore.insertCallback(fixture.workspaceId(), config.id(), "stripe", "evt-duplicate", 10L, "PAYMENT_SUCCEEDED", "PAYMENT_INTENT", callbackBusinessId, "{}", "sig", true, dedupe)).isEmpty();
        var oldCallback = providerStore.insertCallback(fixture.workspaceId(), config.id(), "stripe", "evt-old", 5L, "PAYMENT_SUCCEEDED", "PAYMENT_INTENT", callbackBusinessId, "{}", "sig", true, "evt-old").orElseThrow();
        providerStore.markIgnoredOutOfOrder(fixture.workspaceId(), oldCallback.id());
        assertThat(providerStore.findCallback(fixture.workspaceId(), oldCallback.id())).hasValueSatisfying(callback ->
                assertThat(callback.processingStatus()).isEqualTo("IGNORED_OUT_OF_ORDER"));

        UUID paymentId = UUID.randomUUID();
        RegionReplicationEvent newer = investigationEvent(fixture.workspaceId(), paymentId, "SETTLED", 20, Instant.now());
        RegionReplicationEvent older = investigationEvent(fixture.workspaceId(), paymentId, "PENDING", 10, Instant.now().minusSeconds(60));
        regionReplicationService.apply(objectMapper.writeValueAsString(newer));
        regionReplicationService.apply(objectMapper.writeValueAsString(newer));
        regionReplicationService.apply(objectMapper.writeValueAsString(older));

        assertThat(jdbcTemplate.queryForObject("""
                SELECT status
                FROM region_investigation_read_snapshots
                WHERE workspace_id = ?
                  AND document_id = ?
                """, String.class, fixture.workspaceId(), "PAYMENT_INTENT:" + paymentId)).isEqualTo("SETTLED");
        assertThat(jdbcTemplate.queryForObject("""
                SELECT last_applied_sequence
                FROM region_replication_checkpoints
                WHERE workspace_id = ?
                  AND stream_name = ?
                """, Long.class, fixture.workspaceId(), RegionReplicationService.INVESTIGATION_READ_SNAPSHOT)).isEqualTo(20L);
    }

    private RegionReplicationEvent investigationEvent(UUID workspaceId, UUID paymentId, String status, long sequence, Instant occurredAt) {
        InvestigationDocument document = new InvestigationDocument(
                "PAYMENT_INTENT:" + paymentId,
                workspaceId,
                "PAYMENT_INTENT",
                "PAYMENT_INTENT",
                paymentId,
                UUID.randomUUID(),
                paymentId,
                null,
                null,
                null,
                null,
                "evt-" + sequence,
                "ext-" + sequence,
                "PAYMENT_INTENT",
                paymentId,
                status,
                "USD",
                1000L,
                occurredAt,
                Map.of("paymentIntentId", paymentId.toString(), "status", status));
        return new RegionReplicationEvent(UUID.randomUUID(), sequence, "region-a", "region-b", RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, workspaceId, occurredAt, Map.of("document", document));
    }

    private Fixture fixture(String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-b', 1)", fixture.workspaceId());
        return fixture;
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
