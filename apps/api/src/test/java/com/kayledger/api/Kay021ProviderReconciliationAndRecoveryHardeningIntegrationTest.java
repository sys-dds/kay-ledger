package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
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
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.provider.model.ProviderStatementAmounts;
import com.kayledger.api.provider.model.ProviderStatementTruth;
import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reconciliation.model.ReconciliationItem;
import com.kayledger.api.region.application.RegionFaultService;
import com.kayledger.api.region.application.RegionReplicationService;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.region.config.RegionProperties;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService;
import com.kayledger.api.region.recovery.application.RegionalRecoveryService.RecoveryCommand;
import com.kayledger.api.region.recovery.store.RegionalRecoveryStore;
import com.kayledger.api.region.store.RegionFaultStore;
import com.kayledger.api.region.store.RegionStore;
import com.kayledger.api.reporting.store.ReportingStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

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
class Kay021ProviderReconciliationAndRecoveryHardeningIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> REGION_A_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay021_region_a")
            .withUsername("kay_ledger")
            .withPassword("kay_ledger");

    @Container
    static final PostgreSQLContainer<?> REGION_B_POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay021_region_b")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay021.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay021.replication");
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
    RegionFaultService regionFaultService;

    @Autowired
    RegionReplicationService regionReplicationService;

    @Autowired
    ReconciliationService reconciliationService;

    @Autowired
    ReportingStore reportingStore;

    @Autowired
    InvestigationStore investigationStore;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void invariant_provider_reconciliation_and_recovery_truth_are_durable_scoped_and_auditable() {
        JdbcTemplate regionBJdbc = regionBJdbc();

        Fixture publishFailure = ownedFixture(jdbcTemplate, "kay021-publish-failure");
        regionService.transferOwnership(paymentWrite(publishFailure), "region-b", RegionService.SIMULATED_REGION_FAILOVER);
        regionFaultService.inject(paymentWrite(publishFailure), new RegionFaultService.FaultCommand(
                RegionFaultService.REGIONAL_REPLICATION_PUBLISH_BLOCK,
                "WORKSPACE",
                Map.of(),
                "kay021 publish failure"));
        assertThatThrownBy(() -> regionalRecoveryService.requestRecovery(paymentWrite(publishFailure), new RecoveryCommand(
                null,
                RegionalRecoveryService.REPLAY_OWNERSHIP_TRANSFER,
                "WORKSPACE",
                publishFailure.workspaceId().toString())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blocked");
        assertThat(regionalRecoveryService.listActions(paymentWrite(publishFailure))).noneMatch(action -> "AWAITING_PEER_APPLY".equals(action.status()));

        Fixture zeroInvestigation = ownedFixture(jdbcTemplate, "kay021-zero-investigation");
        assertThatThrownBy(() -> regionalRecoveryService.requestRecovery(paymentWrite(zeroInvestigation), new RecoveryCommand(
                null,
                RegionalRecoveryService.REPLAY_INVESTIGATION_SNAPSHOT,
                "PROVIDER_CALLBACK",
                UUID.randomUUID().toString())))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("No investigation documents");

        Fixture zeroProvider = ownedFixture(jdbcTemplate, "kay021-zero-provider");
        assertThatThrownBy(() -> regionalRecoveryService.requestRecovery(paymentWrite(zeroProvider), new RecoveryCommand(
                null,
                RegionalRecoveryService.REPLAY_PROVIDER_SUMMARY_SNAPSHOT,
                RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT,
                zeroProvider.providerProfileId().toString())))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("No provider summary rows");

        Fixture stale = ownedFixture(jdbcTemplate, "kay021-stale-await");
        var staleAction = new RegionalRecoveryStore(jdbcTemplate).createAction(stale.workspaceId(), null, RegionalRecoveryService.REPLAY_PROVIDER_SUMMARY_SNAPSHOT, RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT, stale.providerProfileId().toString(), stale.ownerId());
        new RegionalRecoveryStore(jdbcTemplate).markActionAwaitingPeerApply(stale.workspaceId(), staleAction.id(), "{\"status\":\"replay_published\"}");
        jdbcTemplate.update("UPDATE regional_recovery_actions SET created_at = now() - interval '2 hours' WHERE id = ?", staleAction.id());
        assertThat(regionalRecoveryService.listActions(paymentWrite(stale))).anySatisfy(action -> {
            assertThat(action.id()).isEqualTo(staleAction.id());
            assertThat(action.status()).isEqualTo("FAILED");
            assertThat(action.resultJson()).contains("peer_apply_expired");
        });

        Fixture summaryReplay = ownedFixture(jdbcTemplate, "kay021-summary-replay");
        fixture(regionBJdbc, summaryReplay);
        regionBJdbc.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", summaryReplay.workspaceId());
        seedSettledPayment(jdbcTemplate, summaryReplay, 12000, 1200, "kay021-summary-replay");
        reportingStore.refreshProviderSummaries(summaryReplay.workspaceId());
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of("kay-ledger.kay021.replication"));
            consumer.poll(Duration.ofMillis(200));
            var action = regionalRecoveryService.requestRecovery(paymentWrite(summaryReplay), new RecoveryCommand(
                    null,
                    RegionalRecoveryService.REPLAY_PROVIDER_SUMMARY_SNAPSHOT,
                    RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT,
                    summaryReplay.providerProfileId().toString()));
            assertThat(action.status()).isEqualTo("AWAITING_PEER_APPLY");
            regionBReplication(regionBJdbc).apply(pollPayload(consumer, RegionReplicationService.PROVIDER_SUMMARY_SNAPSHOT));
            regionReplicationService.apply(pollPayload(consumer, RegionReplicationService.RECOVERY_ACTION_CONFIRMATION));
        }
        assertThat(regionalRecoveryService.listActions(paymentWrite(summaryReplay))).anySatisfy(action -> {
            assertThat(action.actionType()).isEqualTo(RegionalRecoveryService.REPLAY_PROVIDER_SUMMARY_SNAPSHOT);
            assertThat(action.status()).isEqualTo("SUCCEEDED");
            assertThat(action.peerAppliedRegion()).isEqualTo("region-b");
            assertThat(action.peerAppliedAt()).isNotNull();
            assertThat(action.peerConfirmationEventId()).isNotNull();
        });

        Fixture exact = ownedFixture(jdbcTemplate, "kay021-exact");
        seedSettledPayment(jdbcTemplate, exact, 10000, 1000, "kay021-exact");
        reportingStore.refreshProviderSummaries(exact.workspaceId());
        var exactImport = reconciliationService.recordProviderTruth(financeWrite(exact), truth(exact, "kay021-statement-exact", amounts(10000, 1000, 9000, 0, 0, 0, 0)));
        var exactRun = reconciliationService.createRunFromProviderTruth(financeWrite(exact), exactImport.id());
        assertThat(exactRun.unresolvedItemCount()).isZero();
        assertThat(reconciliationService.listItems(financeRead(exact), exactRun.id(), true)).isEmpty();

        var mismatchImport = reconciliationService.recordProviderTruth(financeWrite(exact), truth(exact, "kay021-statement-mismatch", amounts(10100, 1100, 9000, 50, 60, 70, 80)));
        var mismatchRun = reconciliationService.createRunFromProviderTruth(financeWrite(exact), mismatchImport.id());
        List<ReconciliationItem> mismatches = reconciliationService.listItems(financeRead(exact), mismatchRun.id(), true);
        assertThat(mismatches).extracting(ReconciliationItem::mismatchType)
                .contains("SETTLED_GROSS_MISMATCH", "FEE_MISMATCH", "PAYOUT_MISMATCH", "REFUND_MISMATCH", "DISPUTE_EXPOSURE_MISMATCH", "SUBSCRIPTION_REVENUE_MISMATCH");

        Fixture missingInternal = ownedFixture(jdbcTemplate, "kay021-missing-internal");
        var missingInternalImport = reconciliationService.recordProviderTruth(financeWrite(missingInternal), truth(missingInternal, "kay021-missing-internal", amounts(100, 10, 90, 0, 0, 0, 0)));
        var missingInternalRun = reconciliationService.createRunFromProviderTruth(financeWrite(missingInternal), missingInternalImport.id());
        assertThat(reconciliationService.listItems(financeRead(missingInternal), missingInternalRun.id(), true))
                .extracting(ReconciliationItem::mismatchType)
                .containsExactly("MISSING_INTERNAL_SUMMARY");

        var missingProviderImport = reconciliationService.recordProviderTruth(financeWrite(exact), new ProviderStatementTruth(
                exact.providerProfileId(), "USD", LocalDate.parse("2036-01-01"), LocalDate.parse("2036-01-31"), "kay021-missing-provider", null, Map.of()));
        var missingProviderRun = reconciliationService.createRunFromProviderTruth(financeWrite(exact), missingProviderImport.id());
        ReconciliationItem missingProviderItem = reconciliationService.listItems(financeRead(exact), missingProviderRun.id(), true).getFirst();
        assertThat(missingProviderItem.mismatchType()).isEqualTo("MISSING_PROVIDER_TRUTH");

        assertThat(investigationStore.documentsForReference(exact.workspaceId(), "PROVIDER_RECONCILIATION_RUN", mismatchRun.id()))
                .anyMatch(document -> "PROVIDER_RECONCILIATION_RUN".equals(document.referenceType()) && mismatchRun.id().equals(document.referenceId()));
        assertThat(investigationStore.documentsForReference(exact.workspaceId(), "PROVIDER_RECONCILIATION_ITEM", mismatches.getFirst().id()))
                .anyMatch(document -> "PROVIDER_RECONCILIATION_ITEM".equals(document.referenceType()) && mismatches.getFirst().id().equals(document.referenceId()));
        assertThat(investigationStore.documentsForWorkspace(exact.workspaceId()).stream()
                .filter(document -> "PROVIDER_RECONCILIATION_ITEM".equals(document.referenceType()))
                .anyMatch(document -> "SETTLED_GROSS_MISMATCH".equals(document.mismatchType())
                        && exact.providerProfileId().equals(document.providerProfileId())
                        && "USD".equals(document.currencyCode())
                        && "kay021-statement-mismatch".equals(document.externalReference())
                        && "OPEN".equals(document.status()))).isTrue();

        Fixture otherWorkspace = ownedFixture(jdbcTemplate, "kay021-other");
        assertThatThrownBy(() -> reconciliationService.listItems(financeRead(otherWorkspace), mismatchRun.id(), true))
                .isInstanceOf(NotFoundException.class);

        ReconciliationItem resolved = reconciliationService.resolveItem(financeWrite(exact), missingProviderItem.id(), "PROVIDER_STATEMENT_ACCEPTED", "Provider statement controls this reconciliation.");
        assertThat(resolved.status()).isEqualTo("RESOLVED");
        assertThat(resolved.resolvedByActorId()).isEqualTo(exact.ownerId());
        assertThat(resolved.resolutionNote()).contains("Provider statement controls");
        assertThat(resolved.resolvedAt()).isNotNull();
    }

    private Fixture ownedFixture(JdbcTemplate jdbc, String slug) {
        Fixture fixture = fixture(jdbc, slug);
        jdbc.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        return fixture;
    }

    private Fixture fixture(JdbcTemplate jdbc, String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        fixture(jdbc, fixture);
        return fixture;
    }

    private void fixture(JdbcTemplate jdbc, Fixture fixture) {
        jdbc.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.providerActorId(), fixture.slug() + "-provider", "Provider");
        jdbc.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.customerActorId(), fixture.slug() + "-customer", "Customer");
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", UUID.randomUUID(), fixture.workspaceId(), fixture.providerActorId());
        jdbc.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'CUSTOMER')", UUID.randomUUID(), fixture.workspaceId(), fixture.customerActorId());
        jdbc.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", fixture.providerProfileId(), fixture.workspaceId(), fixture.providerActorId());
        jdbc.update("INSERT INTO customer_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Customer')", fixture.customerProfileId(), fixture.workspaceId(), fixture.customerActorId());
    }

    private void seedSettledPayment(JdbcTemplate jdbc, Fixture fixture, long gross, long fee, String reference) {
        jdbc.update("""
                INSERT INTO offerings (id, workspace_id, provider_profile_id, title, status, duration_minutes, offer_type, min_notice_minutes, max_notice_days, slot_interval_minutes)
                VALUES (?, ?, ?, 'KAY021 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, fixture.offeringId(), fixture.workspaceId(), fixture.providerProfileId());
        UUID bookingId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, fixture.workspaceId(), fixture.offeringId(), fixture.providerProfileId(), fixture.customerProfileId(),
                java.sql.Timestamp.from(Instant.parse("2036-07-01T10:00:00Z")),
                java.sql.Timestamp.from(Instant.parse("2036-07-01T11:00:00Z")),
                java.sql.Timestamp.from(Instant.parse("2036-07-01T09:00:00Z")));
        jdbc.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', ?, ?, ?, ?, ?, ?, ?)
                """, UUID.randomUUID(), fixture.workspaceId(), bookingId, fixture.providerProfileId(), gross, fee, gross - fee, gross, gross, gross, reference);
    }

    private ProviderStatementTruth truth(Fixture fixture, String sourceReference, ProviderStatementAmounts amounts) {
        return new ProviderStatementTruth(
                fixture.providerProfileId(),
                "USD",
                LocalDate.parse("2036-01-01"),
                LocalDate.parse("2036-01-31"),
                sourceReference,
                amounts,
                Map.of("sourceReference", sourceReference));
    }

    private ProviderStatementAmounts amounts(long gross, long fee, long net, long payout, long refund, long dispute, long subscription) {
        return new ProviderStatementAmounts(gross, fee, net, payout, refund, dispute, subscription);
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return context(fixture, Set.of("PAYMENT_READ", "PAYMENT_WRITE"));
    }

    private AccessContext financeRead(Fixture fixture) {
        return context(fixture, Set.of("FINANCE_READ"));
    }

    private AccessContext financeWrite(Fixture fixture) {
        return context(fixture, Set.of("FINANCE_READ", "FINANCE_WRITE"));
    }

    private AccessContext context(Fixture fixture, Set<String> scopes) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", scopes, Set.of());
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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kay021-proof-" + UUID.randomUUID());
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
        properties.setReplicationTopic("kay-ledger.kay021.replication");
        RegionStore store = new RegionStore(regionBJdbc);
        RegionFaultService faults = new RegionFaultService(new RegionFaultStore(regionBJdbc), new AccessPolicy(), objectMapper);
        return new RegionReplicationService(store, properties, kafkaTemplate, objectMapper, faults, new RegionalRecoveryStore(regionBJdbc));
    }

    private JdbcTemplate regionBJdbc() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(REGION_B_POSTGRES.getJdbcUrl());
        dataSource.setUsername(REGION_B_POSTGRES.getUsername());
        dataSource.setPassword(REGION_B_POSTGRES.getPassword());
        return new JdbcTemplate(dataSource);
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerActorId, UUID customerActorId, UUID providerProfileId, UUID customerProfileId, UUID offeringId) {
    }
}
