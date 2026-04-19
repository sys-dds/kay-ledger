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
import com.kayledger.api.investigation.application.InvestigationSearchService;
import com.kayledger.api.investigation.model.InvestigationDocument;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionReplicationService.RegionReplicationEvent;
import com.kayledger.api.region.model.RegionReadFreshness;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-b",
        "kay-ledger.region.peer-region-ids=region-a",
        "kay-ledger.region.replication-consumer-enabled=true"
})
class CrossRegionReplicationAndStaleReadIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay017_replication")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay017.replication.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://localhost:1");
        registry.add("kay-ledger.object-storage.endpoint", () -> "http://localhost:1");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay017.region.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Autowired
    InvestigationSearchService investigationSearchService;

    @Autowired
    ReportingService reportingService;

    @Test
    void invariant_replicated_operator_reads_are_local_durable_stale_aware_and_duplicate_safe() throws Exception {
        Fixture fixture = fixture("kay017-replication");
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        UUID paymentId = UUID.randomUUID();
        InvestigationDocument document = new InvestigationDocument(
                "PAYMENT_INTENT:" + paymentId,
                fixture.workspaceId(),
                "PAYMENT_INTENT",
                "PAYMENT_INTENT",
                paymentId,
                fixture.providerProfileId(),
                paymentId,
                null,
                null,
                null,
                null,
                "evt-region-repl",
                "ext-region-repl",
                "PAYMENT_INTENT",
                paymentId,
                "SETTLED",
                "USD",
                2500L,
                Instant.now(),
                Map.of("paymentIntentId", paymentId.toString(), "externalReference", "ext-region-repl"));
        RegionReplicationEvent investigationEvent = event(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT, fixture.workspaceId(), Map.of("document", document));

        regionReplicationService.apply(objectMapper.writeValueAsString(investigationEvent));
        regionReplicationService.apply(objectMapper.writeValueAsString(investigationEvent));

        var hits = investigationSearchService.byReference(paymentRead(fixture), paymentId.toString());
        assertThat(hits).hasSize(1);
        assertThat(hits.getFirst().source()).containsEntry("readSource", "REPLICATED_SNAPSHOT");

        ProviderFinancialSummary summary = new ProviderFinancialSummary(fixture.workspaceId(), fixture.providerProfileId(), "USD", 2500, 250, 2250, 0, 0, 0, 0, 0, Instant.now());
        RegionReplicationEvent summaryEvent = event(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, fixture.workspaceId(), Map.of("summary", summary));
        regionReplicationService.apply(objectMapper.writeValueAsString(summaryEvent));
        regionReplicationService.apply(objectMapper.writeValueAsString(summaryEvent));

        assertThat(reportingService.listSummaries(financeRead(fixture))).hasSize(1);
        RegionReadFreshness freshness = regionReplicationService.freshness(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, fixture.workspaceId());
        assertThat(freshness.readSource()).isEqualTo("REPLICATED_SNAPSHOT");
        assertThat(freshness.lagMillis()).isGreaterThanOrEqualTo(0);
        assertThat(regionReplicationService.checkpoints()).anySatisfy(checkpoint -> {
            assertThat(checkpoint.streamName()).isEqualTo(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT);
            assertThat(checkpoint.lastAppliedEventId()).isEqualTo(summaryEvent.eventId());
        });
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM region_investigation_read_snapshots WHERE workspace_id = ?", Integer.class, fixture.workspaceId())).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM region_provider_summary_snapshots WHERE workspace_id = ?", Integer.class, fixture.workspaceId())).isEqualTo(1);
    }

    private RegionReplicationEvent event(String streamName, UUID workspaceId, Map<String, Object> payload) {
        return new RegionReplicationEvent(UUID.randomUUID(), System.currentTimeMillis(), "region-a", "region-b", streamName, workspaceId, Instant.now(), payload);
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        UUID providerActorId = UUID.randomUUID();
        UUID providerMembershipId = UUID.randomUUID();
        UUID providerProfileId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", providerActorId, slug + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", providerMembershipId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", providerProfileId, workspaceId, providerActorId);
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId);
    }

    private AccessContext paymentRead(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ"), Set.of());
    }

    private AccessContext financeRead(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerProfileId) {
    }
}
