package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.Instant;
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
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.shared.api.InternalFailureException;
import com.kayledger.api.temporal.application.OperatorWorkflowRecord;
import com.kayledger.api.temporal.application.OperatorWorkflowService;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.search.opensearch.endpoint=http://127.0.0.1:1"
})
class OperatorWorkflowTruthfulnessAndStartupFailureIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay016_startup")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay016.startup.events");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    OperatorWorkflowService operatorWorkflowService;

    @Autowired
    ReportingService reportingService;

    @Test
    void invariant_requested_rows_are_not_started_and_startup_failure_marks_both_rows_failed() {
        Fixture fixture = fixture("kay016-startup");
        OperatorWorkflowRecord requested = operatorWorkflowService.createRequested(
                fixture.workspaceId(),
                OperatorWorkflowService.EXPORT,
                OperatorWorkflowService.EXPORT_JOB,
                UUID.randomUUID(),
                OperatorWorkflowService.API,
                fixture.ownerId(),
                1,
                "Requested for timestamp proof.");

        assertThat(requested.requestedAt()).isNotNull();
        assertThat(requested.startedAt()).isNull();
        assertThat(requested.completedAt()).isNull();
        assertThat(requested.status()).isEqualTo("REQUESTED");

        assertThatThrownBy(() -> reportingService.startExport(financeContext(fixture), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT")))
                .isInstanceOf(InternalFailureException.class)
                .hasMessageContaining("Export orchestration could not be started");

        UUID jobId = jdbcTemplate.queryForObject("""
                SELECT id FROM export_jobs
                WHERE workspace_id = ? AND generation_mode = 'TEMPORAL_ASYNC'
                ORDER BY requested_at DESC
                LIMIT 1
                """, UUID.class, fixture.workspaceId());
        String domainStatus = jdbcTemplate.queryForObject("SELECT status FROM export_jobs WHERE id = ?", String.class, jobId);
        Instant domainStarted = instant("SELECT orchestration_started_at FROM export_jobs WHERE id = ?", jobId);
        Instant domainCompleted = instant("SELECT orchestration_completed_at FROM export_jobs WHERE id = ?", jobId);
        String workflowStatus = jdbcTemplate.queryForObject("SELECT status FROM operator_workflows WHERE business_reference_id = ?", String.class, jobId);
        Instant workflowStarted = instant("SELECT started_at FROM operator_workflows WHERE business_reference_id = ?", jobId);
        Instant workflowCompleted = instant("SELECT completed_at FROM operator_workflows WHERE business_reference_id = ?", jobId);
        Integer staleRequested = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM operator_workflows
                WHERE business_reference_id = ? AND status = 'REQUESTED'
                """, Integer.class, jobId);

        assertThat(domainStatus).isEqualTo("FAILED");
        assertThat(domainStarted).isNull();
        assertThat(domainCompleted).isNotNull();
        assertThat(workflowStatus).isEqualTo("FAILED");
        assertThat(workflowStarted).isNull();
        assertThat(workflowCompleted).isNotNull();
        assertThat(staleRequested).isZero();
    }

    private Instant instant(String sql, UUID id) {
        Timestamp timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class, id);
        return timestamp == null ? null : timestamp.toInstant();
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

    private AccessContext financeContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
