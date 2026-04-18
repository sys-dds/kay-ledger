package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.investigation.application.InvestigationIndexingService;
import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.temporal.application.OperatorWorkflowQueryService;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.config.TemporalProperties;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;
import com.kayledger.api.temporal.model.OperatorWorkflowStatus;

import io.temporal.client.WorkflowClient;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false"
})
class OperatorWorkflowScopeAndProgressIntegrationTest {

    private static final String ACCESS_KEY = "kay_ledger";
    private static final String SECRET_KEY = "kay_ledger_secret";
    private static final String BUCKET = "kay-ledger-kay016-progress";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay016_progress")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @Container
    static final GenericContainer<?> OPENSEARCH = new GenericContainer<>(DockerImageName.parse("opensearchproject/opensearch:2.19.1"))
            .withEnv("discovery.type", "single-node")
            .withEnv("plugins.security.disabled", "true")
            .withEnv("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "KayLedger1!")
            .withExposedPorts(9200)
            .waitingFor(Wait.forHttp("/").forPort(9200).forStatusCode(200));

    @Container
    static final GenericContainer<?> MINIO = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server /data")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000).forStatusCode(200));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay016.progress.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-kay016-progress-" + System.nanoTime());
        registry.add("kay-ledger.object-storage.endpoint", OperatorWorkflowScopeAndProgressIntegrationTest::minioEndpoint);
        registry.add("kay-ledger.object-storage.bucket", () -> BUCKET);
        registry.add("kay-ledger.object-storage.access-key", () -> ACCESS_KEY);
        registry.add("kay-ledger.object-storage.secret-key", () -> SECRET_KEY);
        registry.add("kay-ledger.object-storage.path-style-access-enabled", () -> "true");
        registry.add("kay-ledger.temporal.task-queues.operator-workflows", () -> "kay-ledger-kay016-progress");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ReportingService reportingService;

    @Autowired
    ReconciliationService reconciliationService;

    @Autowired
    InvestigationIndexingService investigationIndexingService;

    @Autowired
    OperatorWorkflowQueryService operatorWorkflowQueryService;

    @Test
    void invariant_workflow_status_scopes_tenant_boundaries_and_progress_are_truthful() throws Exception {
        Fixture alpha = fixture("kay016-progress-alpha");
        Fixture beta = fixture("kay016-progress-beta");
        seedPayment(alpha);

        var exportJob = reportingService.startExport(financeWrite(alpha), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT"));
        awaitStatus("export_jobs", exportJob.id(), "SUCCEEDED");
        OperatorWorkflowStatus exportStatus = operatorWorkflowQueryService.findByReference(financeRead(alpha), OperatorWorkflowService.EXPORT, OperatorWorkflowService.EXPORT_JOB, exportJob.id());
        assertThat(exportStatus.status()).isEqualTo("SUCCEEDED");
        assertThat(exportStatus.progressCurrent()).isEqualTo(4);
        assertThat(exportStatus.progressTotal()).isEqualTo(4);
        assertThat(exportStatus.progressUpdateCount()).isGreaterThanOrEqualTo(2);
        int exportRowCount = domainInt("SELECT row_count FROM export_jobs WHERE id = ?", exportJob.id());
        assertThat(exportRowCount).isPositive();
        assertThat(exportStatus.progressCurrent()).isNotEqualTo(exportRowCount);
        assertThatThrownBy(() -> operatorWorkflowQueryService.findByReference(paymentRead(alpha), OperatorWorkflowService.EXPORT, OperatorWorkflowService.EXPORT_JOB, exportJob.id()))
                .isInstanceOf(ForbiddenException.class);

        var run = reconciliationService.startRun(paymentWrite(alpha), new ReconciliationService.RunReconciliationCommand("FULL"));
        awaitStatus("reconciliation_runs", run.id(), "COMPLETED");
        OperatorWorkflowStatus reconciliationStatus = operatorWorkflowQueryService.findByReference(paymentRead(alpha), OperatorWorkflowService.RECONCILIATION, OperatorWorkflowService.RECONCILIATION_RUN, run.id());
        assertThat(reconciliationStatus.progressCurrent()).isEqualTo(4);
        assertThat(reconciliationStatus.progressTotal()).isEqualTo(4);
        assertThat(reconciliationStatus.progressUpdateCount()).isGreaterThanOrEqualTo(2);
        int mismatchCount = domainInt("SELECT mismatch_count FROM reconciliation_runs WHERE id = ?", run.id());
        assertThat(mismatchCount).isNotNegative();
        assertThat(reconciliationStatus.progressCurrent()).isNotEqualTo(mismatchCount);
        assertThatThrownBy(() -> operatorWorkflowQueryService.findByReference(financeRead(alpha), OperatorWorkflowService.RECONCILIATION, OperatorWorkflowService.RECONCILIATION_RUN, run.id()))
                .isInstanceOf(ForbiddenException.class);

        assertThatThrownBy(() -> investigationIndexingService.startReindex(paymentRead(alpha)))
                .isInstanceOf(ForbiddenException.class);
        var reindexJob = investigationIndexingService.startReindex(paymentWrite(alpha));
        awaitStatus("investigation_reindex_jobs", reindexJob.id(), "SUCCEEDED");
        OperatorWorkflowStatus reindexStatus = operatorWorkflowQueryService.findByReference(paymentRead(alpha), OperatorWorkflowService.INVESTIGATION_REINDEX, OperatorWorkflowService.INVESTIGATION_REINDEX_JOB, reindexJob.id());
        assertThat(reindexStatus.progressCurrent()).isEqualTo(3);
        assertThat(reindexStatus.progressTotal()).isEqualTo(3);
        assertThat(reindexStatus.progressUpdateCount()).isGreaterThanOrEqualTo(2);
        assertThat(domainInt("SELECT indexed_count FROM investigation_reindex_jobs WHERE id = ?", reindexJob.id())).isPositive();
        assertThat(domainInt("SELECT failed_count FROM investigation_reindex_jobs WHERE id = ?", reindexJob.id())).isZero();

        assertThat(operatorWorkflowQueryService.list(financeRead(beta), OperatorWorkflowService.EXPORT)).isEmpty();
        assertThat(operatorWorkflowQueryService.list(paymentRead(beta), OperatorWorkflowService.RECONCILIATION)).isEmpty();
        assertThat(operatorWorkflowQueryService.list(paymentRead(beta), OperatorWorkflowService.INVESTIGATION_REINDEX)).isEmpty();
    }

    private void awaitStatus(String table, UUID id, String expected) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        String status = null;
        while (Instant.now().isBefore(deadline)) {
            status = jdbcTemplate.queryForObject("SELECT status FROM " + table + " WHERE id = ?", String.class, id);
            if (expected.equals(status)) {
                return;
            }
            Thread.sleep(200);
        }
        assertThat(status).isEqualTo(expected);
    }

    private int domainInt(String sql, UUID id) {
        Integer value = jdbcTemplate.queryForObject(sql, Integer.class, id);
        return value == null ? 0 : value;
    }

    private void seedPayment(Fixture fixture) {
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', 10000, 1000, 9000, 10000, 10000, 10000, 'kay016-progress')
                """, UUID.randomUUID(), fixture.workspaceId(), fixture.bookingId(), fixture.providerProfileId());
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        UUID providerActorId = UUID.randomUUID();
        UUID providerMembershipId = UUID.randomUUID();
        UUID customerActorId = UUID.randomUUID();
        UUID customerMembershipId = UUID.randomUUID();
        UUID providerProfileId = UUID.randomUUID();
        UUID customerProfileId = UUID.randomUUID();
        UUID offeringId = UUID.randomUUID();
        UUID bookingId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", providerActorId, slug + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", customerActorId, slug + "-customer", "Customer");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", providerMembershipId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'CUSTOMER')", customerMembershipId, workspaceId, customerActorId);
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", providerProfileId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO customer_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Customer')", customerProfileId, workspaceId, customerActorId);
        jdbcTemplate.update("""
                INSERT INTO offerings (id, workspace_id, provider_profile_id, title, status, duration_minutes, offer_type, min_notice_minutes, max_notice_days, slot_interval_minutes)
                VALUES (?, ?, ?, 'KAY016 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId, timestamp("2036-03-01T10:00:00Z"), timestamp("2036-03-01T11:00:00Z"), timestamp("2036-03-01T09:00:00Z"));
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId, bookingId);
    }

    private AccessContext financeWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE", "PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private AccessContext financeRead(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ"), Set.of());
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private AccessContext paymentRead(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ"), Set.of());
    }

    private static Timestamp timestamp(String value) {
        return Timestamp.from(Instant.parse(value));
    }

    private static String minioEndpoint() {
        return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerProfileId, UUID bookingId) {
    }

    @TestConfiguration
    static class TemporalTestConfiguration {

        @Bean(destroyMethod = "")
        TestWorkflowEnvironment testWorkflowEnvironment() {
            return TestWorkflowEnvironment.newInstance();
        }

        @Bean
        WorkflowClient workflowClient(TestWorkflowEnvironment environment) {
            return environment.getWorkflowClient();
        }

        @Bean
        WorkerFactory workerFactory(TestWorkflowEnvironment environment) {
            return environment.getWorkerFactory();
        }

        @Bean
        Worker testOperatorWorker(WorkerFactory workerFactory, TemporalProperties properties, List<TemporalWorkerCustomizer> customizers) {
            Worker worker = workerFactory.newWorker(properties.getTaskQueues().getOperatorWorkflows());
            customizers.forEach(customizer -> customizer.customize(worker));
            return worker;
        }

        @Bean
        SmartLifecycle temporalTestLifecycle(TestWorkflowEnvironment environment) {
            return new SmartLifecycle() {
                private boolean running;

                @Override
                public void start() {
                    environment.start();
                    running = true;
                }

                @Override
                public void stop() {
                    environment.close();
                    running = false;
                }

                @Override
                public boolean isRunning() {
                    return running;
                }
            };
        }
    }
}
