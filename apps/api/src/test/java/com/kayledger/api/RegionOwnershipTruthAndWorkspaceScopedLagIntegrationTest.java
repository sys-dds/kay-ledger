package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionReplicationService.RegionReplicationEvent;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.store.RegionFaultStore;
import com.kayledger.api.region.store.RegionStore;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;

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
class RegionOwnershipTruthAndWorkspaceScopedLagIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> REGION_A_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay018_region_a")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final PostgreSQLContainer<?> REGION_B_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay018_region_b")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", REGION_A_POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", REGION_A_POSTGRES::getUsername);
        registry.add("spring.datasource.password", REGION_A_POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay018.ownership.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay018.region.replication");
    }

    @BeforeAll
    static void migrateSecondRegion() {
        Flyway.configure()
                .dataSource(REGION_B_POSTGRES.getJdbcUrl(), REGION_B_POSTGRES.getUsername(), REGION_B_POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    RegionService regionService;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Autowired
    ReportingService reportingService;

    @Test
    void invariant_ownership_truth_replicates_missing_ownership_fails_closed_and_lag_is_workspace_scoped() throws Exception {
        JdbcTemplate regionBJdbc = regionBJdbc();
        RegionReplicationService regionBReplication = regionBReplication(regionBJdbc);
        Fixture owned = fixture(jdbcTemplate, "kay018-owned");
        Fixture other = fixture(jdbcTemplate, "kay018-other");
        Fixture missing = fixture(jdbcTemplate, "kay018-missing");
        fixture(regionBJdbc, owned);
        fixture(regionBJdbc, other);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", owned.workspaceId());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", other.workspaceId());

        assertThatThrownBy(() -> regionService.requireOwnedForWrite(UUID.randomUUID(), "missing ownership drill"))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("no region ownership record");
        assertThatThrownBy(() -> regionService.ownership(paymentRead(missing)))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("no region ownership record");
        assertThatThrownBy(() -> regionService.transferOwnership(paymentWrite(owned), "region-z", RegionService.SIMULATED_REGION_FAILOVER))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("configured peer region");

        var failover = regionService.transferOwnership(paymentWrite(owned), "region-b", RegionService.SIMULATED_REGION_FAILOVER);
        RegionReplicationEvent ownershipEvent = event(
                RegionReplicationService.WORKSPACE_OWNERSHIP_TRANSFER,
                owned.workspaceId(),
                failover.newEpoch(),
                Map.of(
                        "fromRegion", failover.fromRegion(),
                        "toRegion", failover.toRegion(),
                        "priorEpoch", failover.priorEpoch(),
                        "newEpoch", failover.newEpoch(),
                        "triggerMode", failover.triggerMode()));
        regionBReplication.apply(objectMapper.writeValueAsString(ownershipEvent));

        assertThat(regionService.ownership(owned.workspaceId()).homeRegion()).isEqualTo("region-b");
        assertThat(new RegionStore(regionBJdbc).findOwnership(owned.workspaceId())).hasValueSatisfying(ownership -> {
            assertThat(ownership.homeRegion()).isEqualTo("region-b");
            assertThat(ownership.ownershipEpoch()).isEqualTo(failover.newEpoch());
        });

        ProviderFinancialSummary ownedSummary = summary(owned.workspaceId(), owned.providerProfileId(), 9000, Instant.now());
        regionBReplication.apply(objectMapper.writeValueAsString(event(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, owned.workspaceId(), 9, Map.of("summary", ownedSummary))));
        assertThat(regionBReplication.freshness(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, owned.workspaceId()).readSource()).isEqualTo("REPLICATED_SNAPSHOT");
        assertThat(regionBReplication.freshness(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, other.workspaceId()).readSource()).isEqualTo("NO_REPLICATED_CHECKPOINT");

        assertThatThrownBy(() -> reportingService.refreshAndListSummaries(financeReadOnly(other)))
                .isInstanceOf(ForbiddenException.class);
    }

    private RegionReplicationService regionBReplication(JdbcTemplate regionBJdbc) {
        RegionProperties properties = new RegionProperties();
        properties.setLocalRegionId("region-b");
        properties.setPeerRegionIds(java.util.List.of("region-a"));
        properties.setReplicationConsumerEnabled(true);
        properties.setReplicationProducerEnabled(false);
        RegionStore store = new RegionStore(regionBJdbc);
        RegionFaultService faults = new RegionFaultService(new RegionFaultStore(regionBJdbc), new com.kayledger.api.access.application.AccessPolicy(), objectMapper);
        return new RegionReplicationService(store, properties, null, objectMapper, faults);
    }

    private JdbcTemplate regionBJdbc() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(REGION_B_POSTGRES.getJdbcUrl());
        dataSource.setUsername(REGION_B_POSTGRES.getUsername());
        dataSource.setPassword(REGION_B_POSTGRES.getPassword());
        return new JdbcTemplate(dataSource);
    }

    private RegionReplicationEvent event(String stream, UUID workspaceId, long sequence, Map<String, Object> payload) {
        return new RegionReplicationEvent(UUID.randomUUID(), sequence, "region-a", "region-b", stream, workspaceId, Instant.now(), payload);
    }

    private ProviderFinancialSummary summary(UUID workspaceId, UUID providerProfileId, long gross, Instant refreshedAt) {
        return new ProviderFinancialSummary(workspaceId, providerProfileId, "USD", gross, 100, gross - 100, 0, 0, 0, 0, 0, refreshedAt);
    }

    private Fixture fixture(JdbcTemplate jdbc, String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        fixture(jdbc, fixture);
        return fixture;
    }

    private void fixture(JdbcTemplate jdbc, Fixture fixture) {
        jdbc.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.providerActorId(), fixture.slug() + "-provider", "Provider");
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", fixture.providerMembershipId(), fixture.workspaceId(), fixture.providerActorId());
        jdbc.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", fixture.providerProfileId(), fixture.workspaceId(), fixture.providerActorId());
    }

    private AccessContext paymentRead(Fixture fixture) {
        return context(fixture, Set.of("PAYMENT_READ"));
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return context(fixture, Set.of("PAYMENT_READ", "PAYMENT_WRITE"));
    }

    private AccessContext financeReadOnly(Fixture fixture) {
        return context(fixture, Set.of("FINANCE_READ"));
    }

    private AccessContext context(Fixture fixture, Set<String> scopes) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", scopes, Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerActorId, UUID providerMembershipId, UUID providerProfileId) {
    }
}
