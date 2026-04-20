package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.KafkaTemplate;
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
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService.RecoveryCommand;
import com.kayledger.api.region.recovery.store.RegionalRecoveryStore;
import com.kayledger.api.region.store.RegionFaultStore;
import com.kayledger.api.region.store.RegionStore;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-a",
        "kay-ledger.region.peer-region-ids=region-b",
        "kay-ledger.region.replication-consumer-enabled=true",
        "kay-ledger.region.replication-producer-enabled=true",
        "kay-ledger.search.opensearch.endpoint=http://localhost:1",
        "kay-ledger.object-storage.endpoint=http://localhost:1"
})
class PostFailoverRecoveryWorkflowIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> REGION_A_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay019_region_a")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final PostgreSQLContainer<?> REGION_B_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay019_region_b")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", REGION_A_POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", REGION_A_POSTGRES::getUsername);
        registry.add("spring.datasource.password", REGION_A_POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay019.failover.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay019.failover.replication");
    }

    @BeforeAll
    static void migrateSecondRegion() {
        Flyway.configure()
                .dataSource(REGION_B_POSTGRES.getJdbcUrl(), REGION_B_POSTGRES.getUsername(), REGION_B_POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    RegionService regionService;

    @Autowired
    RegionalRecoveryService regionalRecoveryService;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void invariant_failover_and_snapshot_recovery_complete_only_after_peer_apply_confirmation() {
        JdbcTemplate regionBJdbc = regionBJdbc();
        Fixture fixture = fixture(jdbcTemplate, "kay019-failover");
        fixture(regionBJdbc, fixture);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        regionBJdbc.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        AccessContext context = paymentWrite(fixture);

        var failover = regionService.transferOwnership(context, "region-b", RegionService.SIMULATED_REGION_FAILOVER);
        assertThat(failover.newEpoch()).isEqualTo(2);
        assertThat(new RegionStore(regionBJdbc).findOwnership(fixture.workspaceId())).hasValueSatisfying(ownership ->
                assertThat(ownership.homeRegion()).isEqualTo("region-a"));

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of("kay-ledger.kay019.failover.replication"));
            consumer.poll(Duration.ofMillis(200));
            var action = regionalRecoveryService.requestRecovery(context, new RecoveryCommand(
                    null,
                    RegionalRecoveryService.REPLAY_OWNERSHIP_TRANSFER,
                    "WORKSPACE",
                    fixture.workspaceId().toString()));
            assertThat(action.status()).isEqualTo("AWAITING_PEER_APPLY");

            String payload = pollReplicationPayload(consumer);
            regionBReplication(regionBJdbc).apply(payload);
            String confirmation = pollRecoveryConfirmationPayload(consumer);
            regionReplicationService.apply(confirmation);
        }

        assertThat(new RegionStore(regionBJdbc).findOwnership(fixture.workspaceId())).hasValueSatisfying(ownership -> {
            assertThat(ownership.homeRegion()).isEqualTo("region-b");
            assertThat(ownership.ownershipEpoch()).isEqualTo(2);
        });
        assertThat(new RegionStore(regionBJdbc).listFailoverEvents(fixture.workspaceId())).hasSize(1);
        assertThat(regionalRecoveryService.listActions(context)).anySatisfy(action -> {
            assertThat(action.actionType()).isEqualTo(RegionalRecoveryService.REPLAY_OWNERSHIP_TRANSFER);
            assertThat(action.status()).isEqualTo("SUCCEEDED");
        });

        Fixture snapshotFixture = fixture(jdbcTemplate, "kay020-snapshot");
        fixture(regionBJdbc, snapshotFixture);
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", snapshotFixture.workspaceId());
        regionBJdbc.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", snapshotFixture.workspaceId());
        AccessContext snapshotContext = paymentWrite(snapshotFixture);
        UUID callbackId = providerCallbackFixture(jdbcTemplate, snapshotFixture);
        RegionalRecoveryService peerRecovery = peerRecovery(regionBJdbc);
        assertThat(peerRecovery.scan(snapshotContext).stream()
                .filter(record -> RegionalRecoveryService.REGIONAL_READ_SNAPSHOT_MISSING.equals(record.driftType()))
                .map(record -> record.referenceType()))
                .contains(RegionReplicationService.INVESTIGATION_READ_SNAPSHOT);

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of("kay-ledger.kay019.failover.replication"));
            consumer.poll(Duration.ofMillis(200));
            var action = regionalRecoveryService.requestRecovery(snapshotContext, new RecoveryCommand(
                    null,
                    RegionalRecoveryService.REPLAY_INVESTIGATION_SNAPSHOT,
                    "PROVIDER_CALLBACK",
                    callbackId.toString()));
            assertThat(action.status()).isEqualTo("AWAITING_PEER_APPLY");

            String payload = pollInvestigationSnapshotPayload(consumer);
            regionBReplication(regionBJdbc).apply(payload);
            String confirmation = pollRecoveryConfirmationPayload(consumer);
            regionReplicationService.apply(confirmation);
        }

        assertThat(regionBJdbc.queryForObject("""
                SELECT count(*)
                FROM region_investigation_read_snapshots
                WHERE workspace_id = ?
                  AND target_region = 'region-b'
                  AND reference_type = 'PROVIDER_CALLBACK'
                  AND reference_id = ?
                """, Integer.class, snapshotFixture.workspaceId(), callbackId.toString())).isEqualTo(1);
        assertThat(regionalRecoveryService.listActions(snapshotContext)).anySatisfy(action -> {
            assertThat(action.actionType()).isEqualTo(RegionalRecoveryService.REPLAY_INVESTIGATION_SNAPSHOT);
            assertThat(action.status()).isEqualTo("SUCCEEDED");
        });
    }

    private String pollReplicationPayload(KafkaConsumer<String, String> consumer) {
        return pollPayload(consumer, RegionReplicationService.WORKSPACE_OWNERSHIP_TRANSFER);
    }

    private String pollInvestigationSnapshotPayload(KafkaConsumer<String, String> consumer) {
        return pollPayload(consumer, RegionReplicationService.INVESTIGATION_READ_SNAPSHOT);
    }

    private String pollRecoveryConfirmationPayload(KafkaConsumer<String, String> consumer) {
        return pollPayload(consumer, RegionReplicationService.RECOVERY_ACTION_CONFIRMATION);
    }

    private String pollPayload(KafkaConsumer<String, String> consumer, String marker) {
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));
            for (var record : records) {
                if (record.value().contains(marker)) {
                    return record.value();
                }
            }
        }
        throw new AssertionError("Expected replication message containing " + marker + ".");
    }

    private KafkaConsumer<String, String> consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kay019-failover-proof-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private RegionReplicationService regionBReplication(JdbcTemplate regionBJdbc) {
        RegionProperties properties = new RegionProperties();
        properties.setLocalRegionId("region-b");
        properties.setPeerRegionIds(List.of("region-a"));
        properties.setReplicationConsumerEnabled(true);
        properties.setReplicationProducerEnabled(true);
        properties.setReplicationTopic("kay-ledger.kay019.failover.replication");
        RegionStore store = new RegionStore(regionBJdbc);
        RegionFaultService faults = new RegionFaultService(new RegionFaultStore(regionBJdbc), new AccessPolicy(), objectMapper);
        return new RegionReplicationService(store, properties, kafkaTemplate, objectMapper, faults, new RegionalRecoveryStore(regionBJdbc));
    }

    private RegionalRecoveryService peerRecovery(JdbcTemplate regionBJdbc) {
        RegionProperties properties = new RegionProperties();
        properties.setLocalRegionId("region-b");
        properties.setPeerRegionIds(List.of("region-a"));
        properties.setReplicationConsumerEnabled(true);
        properties.setReplicationProducerEnabled(false);
        properties.setReplicationTopic("kay-ledger.kay019.failover.replication");
        RegionStore store = new RegionStore(regionBJdbc);
        RegionFaultService faults = new RegionFaultService(new RegionFaultStore(regionBJdbc), new AccessPolicy(), objectMapper);
        RegionReplicationService replication = new RegionReplicationService(store, properties, null, objectMapper, faults, new RegionalRecoveryStore(regionBJdbc));
        RegionService service = new RegionService(store, properties, new AccessPolicy(), replication);
        return new RegionalRecoveryService(new RegionalRecoveryStore(regionBJdbc), service, replication, null, null, null, new AccessPolicy(), objectMapper);
    }

    private JdbcTemplate regionBJdbc() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(REGION_B_POSTGRES.getJdbcUrl());
        dataSource.setUsername(REGION_B_POSTGRES.getUsername());
        dataSource.setPassword(REGION_B_POSTGRES.getPassword());
        return new JdbcTemplate(dataSource);
    }

    private Fixture fixture(JdbcTemplate jdbc, String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID());
        fixture(jdbc, fixture);
        return fixture;
    }

    private void fixture(JdbcTemplate jdbc, Fixture fixture) {
        jdbc.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
    }

    private UUID providerCallbackFixture(JdbcTemplate jdbc, Fixture fixture) {
        UUID providerConfigId = UUID.randomUUID();
        UUID callbackId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO provider_configs (id, workspace_id, provider_key, display_name, signing_secret, callback_token)
                VALUES (?, ?, 'kay020-provider', 'KAY020 Provider', 'secret', ?)
                """, providerConfigId, fixture.workspaceId(), "token-" + providerConfigId);
        jdbc.update("""
                INSERT INTO provider_callbacks (
                    id, workspace_id, provider_config_id, provider_key, provider_event_id, provider_sequence,
                    callback_type, business_reference_type, business_reference_id, payload_json,
                    signature_header, signature_verified, dedupe_key, processing_status, applied_at
                )
                VALUES (?, ?, ?, 'kay020-provider', 'evt-kay020-snapshot', 20,
                    'PAYMENT_FAILED', 'PAYMENT_INTENT', ?, '{}'::jsonb,
                    'sig', true, 'kay020-provider:evt-kay020-snapshot', 'APPLIED', now())
                """, callbackId, fixture.workspaceId(), providerConfigId, UUID.randomUUID());
        return callbackId;
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"), Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId) {
    }
}
