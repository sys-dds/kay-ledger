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

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionFaultService.FaultCommand;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.shared.api.NotFoundException;

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
class RegionFaultIsolationAndTruthfulFreshnessIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay019_faults")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay019.faults.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay019.faults.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    RegionFaultService regionFaultService;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Test
    void invariant_region_scope_faults_are_workspace_tenant_safe_and_owner_reads_are_labeled_local_truth() {
        Fixture first = fixture("kay019-fault-a", "region-a");
        Fixture second = fixture("kay019-fault-b", "region-a");
        var fault = regionFaultService.inject(paymentWrite(first), new FaultCommand(
                RegionFaultService.REGIONAL_REPLICATION_APPLY_BLOCK,
                "REGION",
                Map.of("drill", "tenant-safe"),
                "tenant-safe region fault drill"));

        assertThat(regionFaultService.active(paymentRead(first))).extracting("id").contains(fault.id());
        assertThat(fault.scope()).isEqualTo("WORKSPACE");
        assertThat(regionFaultService.active(paymentRead(second))).extracting("id").doesNotContain(fault.id());
        assertThatThrownBy(() -> regionFaultService.clear(paymentWrite(second), fault.id()))
                .isInstanceOf(NotFoundException.class);

        var freshness = regionReplicationService.freshness(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, first.workspaceId());
        assertThat(freshness.readSource()).isEqualTo("LOCAL_SOURCE_OF_TRUTH");
        assertThat(freshness.sourceRegion()).isEqualTo("region-a");
        assertThat(freshness.lagMillis()).isZero();

        Fixture remote = fixture("kay019-fault-remote", "region-b");
        assertThat(regionReplicationService.freshness(RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, remote.workspaceId()).readSource())
                .isEqualTo("NO_REPLICATED_CHECKPOINT");
    }

    private Fixture fixture(String slug, String homeRegion) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, ?, 1)", fixture.workspaceId(), homeRegion);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        return fixture;
    }

    private AccessContext paymentRead(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ"), Set.of());
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
