package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

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
import com.kayledger.api.reporting.application.ObjectStorageProperties;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.temporal.application.OperatorWorkflowQueryService;
import com.kayledger.api.temporal.application.OperatorWorkflowService;
import com.kayledger.api.temporal.config.TemporalProperties;
import com.kayledger.api.temporal.config.TemporalWorkerCustomizer;

import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.client.WorkflowClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false"
})
class Kay015TemporalOperatorWorkflowsIntegrationTest {

    private static final String ACCESS_KEY = "kay_ledger";
    private static final String SECRET_KEY = "kay_ledger_secret";
    private static final String BUCKET = "kay-ledger-kay015-workflows";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay015_workflows")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay015.workflow.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://" + OPENSEARCH.getHost() + ":" + OPENSEARCH.getMappedPort(9200));
        registry.add("kay-ledger.search.opensearch.investigation-index", () -> "kay-ledger-kay015-workflows-" + System.nanoTime());
        registry.add("kay-ledger.object-storage.endpoint", Kay015TemporalOperatorWorkflowsIntegrationTest::minioEndpoint);
        registry.add("kay-ledger.object-storage.bucket", () -> BUCKET);
        registry.add("kay-ledger.object-storage.access-key", () -> ACCESS_KEY);
        registry.add("kay-ledger.object-storage.secret-key", () -> SECRET_KEY);
        registry.add("kay-ledger.object-storage.path-style-access-enabled", () -> "true");
        registry.add("kay-ledger.temporal.task-queues.operator-workflows", () -> "kay-ledger-kay015-workflows");
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

    @Autowired
    ObjectStorageProperties objectStorageProperties;

    @Test
    void invariant_async_operator_workflows_complete_truthfully_and_stay_tenant_scoped() throws Exception {
        Fixture alpha = fixture("kay015-workflow-alpha");
        Fixture beta = fixture("kay015-workflow-beta");
        seedSummary(alpha);
        seedPayment(alpha);

        var exportJob = reportingService.startExport(financeContext(alpha), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT"));
        awaitStatus("export_jobs", exportJob.id(), "SUCCEEDED");
        String storageKey = jdbcTemplate.queryForObject("SELECT storage_key FROM export_jobs WHERE id = ?", String.class, exportJob.id());
        assertObjectExists(storageKey);
        assertWorkflow(alpha, OperatorWorkflowService.EXPORT, OperatorWorkflowService.EXPORT_JOB, exportJob.id(), "SUCCEEDED");

        var reconciliationRun = reconciliationService.startRun(paymentContext(alpha), new ReconciliationService.RunReconciliationCommand("FULL"));
        awaitStatus("reconciliation_runs", reconciliationRun.id(), "COMPLETED");
        assertWorkflow(alpha, OperatorWorkflowService.RECONCILIATION, OperatorWorkflowService.RECONCILIATION_RUN, reconciliationRun.id(), "SUCCEEDED");

        var reindexJob = investigationIndexingService.startReindex(paymentContext(alpha));
        awaitStatus("investigation_reindex_jobs", reindexJob.id(), "SUCCEEDED");
        assertWorkflow(alpha, OperatorWorkflowService.INVESTIGATION_REINDEX, OperatorWorkflowService.INVESTIGATION_REINDEX_JOB, reindexJob.id(), "SUCCEEDED");

        assertThat(operatorWorkflowQueryService.list(paymentContext(beta), OperatorWorkflowService.EXPORT)).isEmpty();
        assertThat(operatorWorkflowQueryService.list(paymentContext(beta), OperatorWorkflowService.RECONCILIATION)).isEmpty();
        assertThat(operatorWorkflowQueryService.list(paymentContext(beta), OperatorWorkflowService.INVESTIGATION_REINDEX)).isEmpty();
    }

    private void assertWorkflow(Fixture fixture, String workflowType, String referenceType, UUID referenceId, String status) {
        assertThat(operatorWorkflowQueryService.findByReference(paymentContext(fixture), workflowType, referenceType, referenceId).status())
                .isEqualTo(status);
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

    private void assertObjectExists(String storageKey) {
        try (S3Client client = S3Client.builder()
                .endpointOverride(java.net.URI.create(minioEndpoint()))
                .region(Region.of(objectStorageProperties.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build()) {
            assertThat(client.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(storageKey).build()).contentLength()).isGreaterThan(0);
        }
    }

    private void seedSummary(Fixture fixture) {
        jdbcTemplate.update("""
                INSERT INTO financial_provider_summaries (
                    workspace_id, provider_profile_id, currency_code, settled_gross_amount_minor,
                    fee_amount_minor, net_earnings_amount_minor, current_payout_requested_amount_minor,
                    payout_succeeded_amount_minor, refund_amount_minor, active_dispute_exposure_amount_minor,
                    settled_subscription_net_revenue_amount_minor
                )
                VALUES (?, ?, 'USD', 10000, 1000, 9000, 0, 0, 0, 0, 0)
                """, fixture.workspaceId(), fixture.providerProfileId());
    }

    private void seedPayment(Fixture fixture) {
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', 10000, 1000, 9000, 10000, 10000, 10000, 'kay015-reindex')
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
                VALUES (?, ?, ?, 'KAY015 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, offeringId, workspaceId, providerProfileId);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, workspaceId, offeringId, providerProfileId, customerProfileId, timestamp("2036-01-01T10:00:00Z"), timestamp("2036-01-01T11:00:00Z"), timestamp("2036-01-01T09:00:00Z"));
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId, bookingId);
    }

    private AccessContext paymentContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private AccessContext financeContext(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE", "PAYMENT_READ"), Set.of());
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
