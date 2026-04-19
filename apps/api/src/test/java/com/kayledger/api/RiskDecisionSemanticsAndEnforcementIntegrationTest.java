package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import com.kayledger.api.risk.application.RiskService;
import com.kayledger.api.risk.store.RiskStore;
import com.kayledger.api.shared.api.BadRequestException;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class RiskDecisionSemanticsAndEnforcementIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_risk_test")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.risk.test.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://localhost:1");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    RiskStore riskStore;

    @Autowired
    RiskService riskService;

    @Test
    void invariant_block_is_active_operator_readable_and_exits_only_through_later_decision() {
        Fixture fixture = fixture("risk-alpha");
        UUID providerProfileId = UUID.randomUUID();
        var flag = riskStore.upsertFlag(fixture.workspaceId(), "PROVIDER_PROFILE", providerProfileId, "TEST_BLOCK_SCOPE", "HIGH", "test block", 1);
        var review = riskStore.ensureReview(fixture.workspaceId(), flag.id());

        var block = riskService.decide(context(fixture), review.id(), new RiskService.DecisionCommand("BLOCK", "enforce payout hold"));

        assertThat(block.outcome()).isEqualTo("BLOCK");
        assertThat(riskStore.listFlags(fixture.workspaceId())).singleElement().satisfies(blockedFlag -> assertThat(blockedFlag.status()).isEqualTo("BLOCKED"));
        assertThat(riskStore.listReviewQueue(fixture.workspaceId())).singleElement().satisfies(blockedReview -> assertThat(blockedReview.status()).isEqualTo("BLOCKED"));
        assertThatThrownBy(() -> riskService.requireNotBlocked(fixture.workspaceId(), "PROVIDER_PROFILE", providerProfileId))
                .isInstanceOf(BadRequestException.class);
        assertThat(riskService.enforcementScope(context(fixture)).blockedMutationScopes())
                .containsKeys("PROVIDER_PROFILE", "PAYOUT_REQUEST");

        riskService.decide(context(fixture), review.id(), new RiskService.DecisionCommand("ALLOW", "block cleared"));

        assertThatCode(() -> riskService.requireNotBlocked(fixture.workspaceId(), "PROVIDER_PROFILE", providerProfileId))
                .doesNotThrowAnyException();
        assertThat(riskStore.listFlags(fixture.workspaceId())).singleElement().satisfies(resolvedFlag -> assertThat(resolvedFlag.status()).isEqualTo("RESOLVED"));
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

    private AccessContext context(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
