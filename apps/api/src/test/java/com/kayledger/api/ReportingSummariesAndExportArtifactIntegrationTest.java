package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import com.kayledger.api.reporting.application.ObjectStorageProperties;
import com.kayledger.api.reporting.application.ReportingService;
import com.kayledger.api.reporting.model.ProviderFinancialSummary;
import com.kayledger.api.shared.api.InternalFailureException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class ReportingSummariesAndExportArtifactIntegrationTest {

    private static final String ACCESS_KEY = "kay_ledger";
    private static final String SECRET_KEY = "kay_ledger_secret";
    private static final String BUCKET = "kay-ledger-reporting-test-exports";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_reporting_test")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.reporting.test.events");
        registry.add("kay-ledger.search.opensearch.endpoint", () -> "http://localhost:1");
        registry.add("kay-ledger.object-storage.endpoint", ReportingSummariesAndExportArtifactIntegrationTest::minioEndpoint);
        registry.add("kay-ledger.object-storage.bucket", () -> BUCKET);
        registry.add("kay-ledger.object-storage.access-key", () -> ACCESS_KEY);
        registry.add("kay-ledger.object-storage.secret-key", () -> SECRET_KEY);
        registry.add("kay-ledger.object-storage.path-style-access-enabled", () -> "true");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ReportingService reportingService;

    @Autowired
    ObjectStorageProperties objectStorageProperties;

    @Test
    void invariant_summary_names_export_storage_failure_and_tenant_isolation_are_truthful() {
        Fixture alpha = fixture("reporting-alpha");
        Fixture beta = fixture("reporting-beta");
        jdbcTemplate.update("""
                INSERT INTO financial_provider_summaries (
                    workspace_id, provider_profile_id, currency_code, settled_gross_amount_minor,
                    fee_amount_minor, net_earnings_amount_minor, current_payout_requested_amount_minor,
                    payout_succeeded_amount_minor, refund_amount_minor, active_dispute_exposure_amount_minor,
                    settled_subscription_net_revenue_amount_minor
                )
                VALUES (?, ?, 'USD', 10000, 1000, 9000, 3000, 2000, 500, 700, 8000)
                """, alpha.workspaceId(), alpha.providerProfileId());

        ProviderFinancialSummary summary = reportingService.listSummaries(context(alpha)).getFirst();
        assertThat(summary.currentPayoutRequestedAmountMinor()).isEqualTo(3000);
        assertThat(summary.activeDisputeExposureAmountMinor()).isEqualTo(700);
        assertThat(summary.settledSubscriptionNetRevenueAmountMinor()).isEqualTo(8000);
        assertThat(reportingService.listSummaries(context(beta))).isEmpty();

        var export = reportingService.generateSynchronousExport(context(alpha), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT"));

        assertThat(export.status()).isEqualTo("SUCCEEDED");
        assertThat(export.generationMode()).isEqualTo("SYNCHRONOUS");
        assertThat(reportingService.listJobs(context(alpha))).isNotEmpty();
        var artifact = reportingService.listArtifacts(context(alpha)).getFirst();
        assertThat(artifact.workspaceId()).isEqualTo(alpha.workspaceId());
        assertThat(reportingService.listArtifacts(context(beta))).isEmpty();
        assertObjectExists(artifact.storageKey());

        objectStorageProperties.setBucket("Invalid_Bucket_Name");
        assertThatThrownBy(() -> reportingService.generateSynchronousExport(context(alpha), new ReportingService.ExportRequestCommand("PROVIDER_STATEMENT")))
                .isInstanceOf(InternalFailureException.class);
        assertThat(reportingService.listJobs(context(alpha))).anySatisfy(job -> assertThat(job.status()).isEqualTo("FAILED"));
    }

    private Fixture fixture(String slug) {
        UUID workspaceId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        UUID membershipId = UUID.randomUUID();
        UUID providerActorId = UUID.randomUUID();
        UUID providerMembershipId = UUID.randomUUID();
        UUID providerProfileId = UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", workspaceId, slug, slug);
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", ownerId, slug + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", providerActorId, slug + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", membershipId, workspaceId, ownerId);
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", providerMembershipId, workspaceId, providerActorId);
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", providerProfileId, workspaceId, providerActorId);
        return new Fixture(workspaceId, slug, ownerId, membershipId, providerProfileId);
    }

    private AccessContext context(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE"), Set.of());
    }

    private void assertObjectExists(String storageKey) {
        try (S3Client client = S3Client.builder()
                .endpointOverride(URI.create(minioEndpoint()))
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .build()) {
            assertThat(client.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(storageKey).build()).contentLength()).isGreaterThan(0);
        }
    }

    private static String minioEndpoint() {
        return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerProfileId) {
    }
}
