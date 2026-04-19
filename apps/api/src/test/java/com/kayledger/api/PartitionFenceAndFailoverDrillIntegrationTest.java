package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionFaultService.FaultCommand;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.store.RegionFaultStore;
import com.kayledger.api.region.store.RegionStore;
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
class PartitionFenceAndFailoverDrillIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay018_partition")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay018.partition.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay018.region.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    RegionService regionService;

    @Autowired
    RegionFaultService regionFaultService;

    @Test
    void invariant_partition_drills_keep_fencing_and_failover_failback_epoch_truthful() {
        Fixture fixture = fixture("kay018-partition");
        AccessContext context = paymentWrite(fixture);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());

        regionFaultService.inject(context, new FaultCommand(RegionFaultService.REGIONAL_REPLICATION_PUBLISH_BLOCK, "WORKSPACE", Map.of(), "partition drill"));
        assertThat(regionFaultService.active(context)).extracting("faultType").contains(RegionFaultService.REGIONAL_REPLICATION_PUBLISH_BLOCK);

        var failover = regionService.transferOwnership(context, "region-b", RegionService.SIMULATED_REGION_FAILOVER);
        assertThat(failover.priorEpoch()).isEqualTo(1);
        assertThat(failover.newEpoch()).isEqualTo(2);
        assertThatThrownBy(() -> regionService.requireOwnedForWrite(context, "old-region partition write"))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("owned by region region-b");

        RegionService regionBService = regionServiceFor("region-b", "region-a");
        assertThat(regionBService.requireOwnedForWrite(context, "new-region partition write").homeRegion()).isEqualTo("region-b");

        var failback = regionBService.transferOwnership(context, "region-a", RegionService.MANUAL_FAILBACK);
        assertThat(failback.priorEpoch()).isEqualTo(2);
        assertThat(failback.newEpoch()).isEqualTo(3);
        assertThat(regionService.requireOwnedForWrite(context, "post-failback write").homeRegion()).isEqualTo("region-a");
        assertThat(regionService.failoverEvents(fixture.workspaceId())).hasSize(2);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM information_schema.tables
                WHERE table_name LIKE '%post_failover%'
                   OR table_name LIKE '%failover_reconciliation%'
                """, Integer.class)).isZero();
    }

    private RegionService regionServiceFor(String localRegion, String peerRegion) {
        RegionProperties properties = new RegionProperties();
        properties.setLocalRegionId(localRegion);
        properties.setPeerRegionIds(java.util.List.of(peerRegion));
        properties.setReplicationProducerEnabled(false);
        properties.setReplicationConsumerEnabled(false);
        RegionStore store = new RegionStore(jdbcTemplate);
        RegionFaultService faults = new RegionFaultService(new RegionFaultStore(jdbcTemplate), new AccessPolicy(), objectMapper);
        RegionReplicationService replication = new RegionReplicationService(store, properties, null, objectMapper, faults);
        return new RegionService(store, properties, new AccessPolicy(), replication);
    }

    private Fixture fixture(String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        return fixture;
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
