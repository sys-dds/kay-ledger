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
import com.kayledger.api.region.model.WorkspaceRegionOwnership;
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
class WorkspaceRegionOwnershipAndWriteFencingIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay017_ownership")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay017.ownership.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://localhost:1");
        registry.add("kay-ledger.object-storage.endpoint", () -> "http://localhost:1");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    RegionService regionService;

    @Test
    void invariant_workspace_home_region_fences_partitioned_active_active_writes() {
        Fixture fixture = fixture("kay017-ownership");
        AccessContext context = paymentWrite(fixture);

        WorkspaceRegionOwnership ownership = regionService.requireOwnedForWrite(context, "export request start");
        assertThat(ownership.homeRegion()).isEqualTo("region-a");
        assertThat(ownership.ownershipEpoch()).isEqualTo(1);

        jdbcTemplate.update("UPDATE workspace_region_ownership SET home_region = 'region-b', ownership_epoch = ownership_epoch + 1 WHERE workspace_id = ?", fixture.workspaceId());
        assertThatThrownBy(() -> regionService.requireOwnedForWrite(context, "export request start"))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("owned by region region-b");

        jdbcTemplate.update("UPDATE workspace_region_ownership SET home_region = 'region-a', ownership_epoch = ownership_epoch + 1 WHERE workspace_id = ?", fixture.workspaceId());
        assertThatThrownBy(() -> regionService.requireOwnedForWrite(fixture.workspaceId(), ownership.ownershipEpoch(), "stale ownership write"))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("epoch is stale");
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", workspaceId);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        return new Fixture(workspaceId, slug, ownerId, membershipId);
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE", "FINANCE_READ", "FINANCE_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
