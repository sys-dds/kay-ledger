package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.shared.api.ForbiddenException;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-a",
        "kay-ledger.region.peer-region-ids=region-b",
        "kay-ledger.region.replication-consumer-enabled=false"
})
class WorkspaceFailoverOwnershipTransferIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay017_failover")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay017.failover.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://localhost:1");
        registry.add("kay-ledger.object-storage.endpoint", () -> "http://localhost:1");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    RegionService regionService;

    @Autowired
    RegionProperties regionProperties;

    @Test
    void invariant_workspace_failover_transfers_ownership_epoch_and_audit_without_repair_claim() {
        Fixture fixture = fixture("kay017-failover");
        AccessContext context = paymentWrite(fixture);
        long priorEpoch = regionService.ensureWorkspaceOwnership(fixture.workspaceId()).ownershipEpoch();

        var event = regionService.transferOwnership(context, "region-b", RegionService.SIMULATED_REGION_FAILOVER);
        assertThat(event.fromRegion()).isEqualTo("region-a");
        assertThat(event.toRegion()).isEqualTo("region-b");
        assertThat(event.priorEpoch()).isEqualTo(priorEpoch);
        assertThat(event.newEpoch()).isEqualTo(priorEpoch + 1);

        assertThat(regionService.ownership(fixture.workspaceId()).homeRegion()).isEqualTo("region-b");
        assertThat(regionService.ownership(fixture.workspaceId()).ownershipEpoch()).isEqualTo(priorEpoch + 1);
        assertThatThrownBy(() -> regionService.requireOwnedForWrite(context, "post-failover old-region write"))
                .isInstanceOf(ForbiddenException.class);

        regionProperties.setLocalRegionId("region-b");
        assertThat(regionService.requireOwnedForWrite(context, "post-failover new-region write").homeRegion()).isEqualTo("region-b");

        assertThat(regionService.failoverEvents(fixture.workspaceId())).hasSize(1);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM information_schema.tables
                WHERE table_name LIKE '%post_failover%'
                   OR table_name LIKE '%failover_reconciliation%'
                """, Integer.class)).isZero();
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        return new Fixture(workspaceId, slug, ownerId, membershipId);
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
