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
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.reconciliation.application.ReconciliationService;
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

    @Autowired
    ReconciliationService reconciliationService;

    @Autowired
    InvestigationIndexingService investigationIndexingService;

    @Test
    void invariant_requested_rows_are_not_started_and_all_startup_failures_mark_both_rows_failed() {
        Fixture fixture = fixture("kay016-startup");
        assertStartedAtHasNoDefault("operator_workflows");
        assertStartedAtHasNoDefault("investigation_reindex_jobs");
        assertStartedAtHasNoDefault("reconciliation_runs");

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
        UUID jobId = latestId("export_jobs", fixture.workspaceId(), "requested_at", "generation_mode = 'TEMPORAL_ASYNC'");
        assertFailedStartup(
                "export_jobs",
                jobId,
                "orchestration_started_at",
                "orchestration_completed_at",
                OperatorWorkflowService.EXPORT_JOB);

        assertThatThrownBy(() -> reconciliationService.startRun(paymentContext(fixture), new ReconciliationService.RunReconciliationCommand("FULL")))
                .isInstanceOf(InternalFailureException.class)
                .hasMessageContaining("Reconciliation orchestration could not be started");
        UUID runId = latestId("reconciliation_runs", fixture.workspaceId(), "requested_at", "status = 'FAILED'");
        assertFailedStartup(
                "reconciliation_runs",
                runId,
                "started_at",
                "completed_at",
                OperatorWorkflowService.RECONCILIATION_RUN);

        assertThatThrownBy(() -> investigationIndexingService.startReindex(paymentContext(fixture)))
                .isInstanceOf(InternalFailureException.class)
                .hasMessageContaining("Investigation reindex orchestration could not be started");
        UUID reindexJobId = latestId("investigation_reindex_jobs", fixture.workspaceId(), "requested_at", "status = 'FAILED'");
        assertFailedStartup(
                "investigation_reindex_jobs",
                reindexJobId,
                "started_at",
                "completed_at",
                OperatorWorkflowService.INVESTIGATION_REINDEX_JOB);
    }

    private Instant instant(String sql, Object... args) {
        Timestamp timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class, args);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private void assertStartedAtHasNoDefault(String table) {
        String defaultValue = jdbcTemplate.queryForObject("""
                SELECT column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ?
                  AND column_name = 'started_at'
                """, String.class, table);
        assertThat(defaultValue).isNull();
    }

    private UUID latestId(String table, UUID workspaceId, String orderColumn, String filter) {
        return jdbcTemplate.queryForObject("""
                SELECT id FROM %s
                WHERE workspace_id = ? AND %s
                ORDER BY %s DESC
                LIMIT 1
                """.formatted(table, filter, orderColumn), UUID.class, workspaceId);
    }

    private void assertFailedStartup(String table, UUID domainId, String startedColumn, String completedColumn, String referenceType) {
        String domainStatus = jdbcTemplate.queryForObject("SELECT status FROM " + table + " WHERE id = ?", String.class, domainId);
        Instant domainStarted = instant("SELECT " + startedColumn + " FROM " + table + " WHERE id = ?", domainId);
        Instant domainCompleted = instant("SELECT " + completedColumn + " FROM " + table + " WHERE id = ?", domainId);
        String workflowStatus = jdbcTemplate.queryForObject("SELECT status FROM operator_workflows WHERE business_reference_type = ? AND business_reference_id = ?", String.class, referenceType, domainId);
        Instant workflowStarted = instant("SELECT started_at FROM operator_workflows WHERE business_reference_type = ? AND business_reference_id = ?", referenceType, domainId);
        Instant workflowCompleted = instant("SELECT completed_at FROM operator_workflows WHERE business_reference_type = ? AND business_reference_id = ?", referenceType, domainId);
        Integer staleRequested = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM operator_workflows
                WHERE business_reference_type = ?
                  AND business_reference_id = ?
                  AND status = 'REQUESTED'
                """, Integer.class, referenceType, domainId);

        assertThat(domainStatus).isEqualTo("FAILED");
        assertThat(domainStarted).isNull();
        assertThat(domainCompleted).isNotNull();
        assertThat(workflowStatus).isEqualTo("FAILED");
        assertThat(workflowStarted).isNull();
        assertThat(workflowCompleted).isNotNull();
        assertThat(staleRequested).isZero();
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

    private AccessContext paymentContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
