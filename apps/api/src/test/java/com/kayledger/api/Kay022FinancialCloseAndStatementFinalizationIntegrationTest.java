package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
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
import com.kayledger.api.close.application.FinancialCloseService;
import com.kayledger.api.close.model.FinalizedProviderStatement;
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.provider.model.ProviderStatementAmounts;
import com.kayledger.api.provider.model.ProviderStatementTruth;
import com.kayledger.api.reconciliation.application.ReconciliationService;
import com.kayledger.api.reconciliation.model.ProviderTruthImport;
import com.kayledger.api.reconciliation.model.ReconciliationItem;
import com.kayledger.api.reconciliation.store.ReconciliationStore;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(properties = {
        "kay-ledger.temporal.enabled=false",
        "kay-ledger.temporal.worker-enabled=false",
        "kay-ledger.region.local-region-id=region-a",
        "kay-ledger.region.peer-region-ids=region-b",
        "kay-ledger.region.replication-consumer-enabled=false",
        "kay-ledger.region.replication-producer-enabled=true",
        "kay-ledger.search.opensearch.endpoint=http://localhost:1",
        "kay-ledger.object-storage.endpoint=http://localhost:1"
})
class Kay022FinancialCloseAndStatementFinalizationIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay022")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay022.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay022.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ReconciliationService reconciliationService;

    @Autowired
    ReconciliationStore reconciliationStore;

    @Autowired
    FinancialCloseService financialCloseService;

    @Autowired
    PaymentService paymentService;

    @Autowired
    InvestigationStore investigationStore;

    @Test
    void invariant_financial_close_reconciliation_periods_and_reopen_audit_are_durable() {
        Fixture fixture = ownedFixture("kay022-main");
        seedSettledPayment(fixture, 10000, 1000, "kay022-period-payment");

        ProviderTruthImport matchedImport = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay022-matched", statementStart(), statementEnd(), amounts(10000, 1000, 9000, 0, 0, 0, 0)));
        var matchedRun = reconciliationService.createRunFromProviderTruth(financeWrite(fixture), matchedImport.id());
        assertThat(matchedRun.status()).isEqualTo("MATCHED");
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), matchedImport.id()).orElseThrow().status()).isEqualTo("MATCHED");

        ProviderTruthImport wrongPeriodImport = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay022-wrong-period", LocalDate.parse("2040-01-01"), LocalDate.parse("2040-01-31"), amounts(10000, 1000, 9000, 0, 0, 0, 0)));
        var wrongPeriodRun = reconciliationService.createRunFromProviderTruth(financeWrite(fixture), wrongPeriodImport.id());
        assertThat(reconciliationService.listItems(financeRead(fixture), wrongPeriodRun.id(), true))
                .extracting(ReconciliationItem::mismatchType)
                .containsExactly("MISSING_INTERNAL_SUMMARY");

        ProviderTruthImport firstVersion = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay022-versioned", statementStart(), statementEnd(), amounts(10000, 1000, 9000, 0, 0, 0, 0)));
        ProviderTruthImport secondVersion = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay022-versioned", statementStart(), statementEnd(), amounts(10100, 1000, 9100, 0, 0, 0, 0)));
        assertThat(secondVersion.importVersion()).isEqualTo(firstVersion.importVersion() + 1);
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), firstVersion.id()).orElseThrow().status()).isEqualTo("SUPERSEDED");
        assertThatThrownBy(() -> reconciliationService.createRunFromProviderTruth(financeWrite(fixture), firstVersion.id()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Superseded");

        var mismatchedRun = reconciliationService.createRunFromProviderTruth(financeWrite(fixture), secondVersion.id());
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), secondVersion.id()).orElseThrow().status()).isEqualTo("MISMATCHED");
        List<ReconciliationItem> mismatchItems = reconciliationService.listItems(financeRead(fixture), mismatchedRun.id(), true);
        ReconciliationItem mismatch = mismatchItems.getFirst();
        for (ReconciliationItem item : mismatchItems) {
            reconciliationService.resolveItem(financeWrite(fixture), item.id(), "PROVIDER_CORRECTION_ACCEPTED", "Operator accepted provider correction.");
        }
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), secondVersion.id()).orElseThrow().status()).isEqualTo("RESOLVED");
        reconciliationService.reopenItem(financeWrite(fixture), mismatch.id(), "Provider sent a replacement statement.");
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), secondVersion.id()).orElseThrow().status()).isEqualTo("MISMATCHED");
        assertThat(reconciliationService.listItemEvents(financeRead(fixture), mismatch.id()))
                .extracting(event -> event.eventType())
                .containsExactly("RESOLVED", "REOPENED");

        var period = financialCloseService.openPeriod(financeWrite(fixture), statementStart(), statementEnd());
        var close = financialCloseService.closePeriod(financeWrite(fixture), period.id(), "Month-end provider settlement close.");
        assertThat(close.period().status()).isEqualTo("CLOSED");
        assertThat(close.finalizedStatements()).hasSize(1);
        FinalizedProviderStatement statement = close.finalizedStatements().getFirst();
        assertThat(statement.settledGrossAmountMinor()).isEqualTo(10000);
        assertThat(statement.periodStart()).isEqualTo(statementStart());
        assertThat(statement.periodEnd()).isEqualTo(statementEnd());

        UUID capturedIntentId = seedCapturedPayment(fixture, 5000, 500, "kay022-blocked-payment");
        assertThatThrownBy(() -> paymentService.settle(paymentWrite(fixture), capturedIntentId, new PaymentService.AmountCommand(5000L, "blocked-after-close")))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("closed accounting period");

        var reopened = financialCloseService.reopenPeriod(financeWrite(fixture), period.id(), "Provider sent a late correction.");
        assertThat(reopened.status()).isEqualTo("OPEN");
        assertThat(financialCloseService.auditEvents(financeRead(fixture), period.id()))
                .extracting(event -> event.eventType())
                .contains("PERIOD_OPENED", "PERIOD_CLOSED", "STATEMENT_FINALIZED", "PERIOD_REOPENED");

        assertThat(investigationStore.documentsForWorkspace(fixture.workspaceId()))
                .anyMatch(document -> "ACCOUNTING_PERIOD".equals(document.referenceType())
                        && period.id().equals(document.referenceId())
                        && "OPEN".equals(document.status())
                        && statementStart().toString().equals(document.periodStart()))
                .anyMatch(document -> "FINALIZED_PROVIDER_STATEMENT".equals(document.referenceType())
                        && statement.id().equals(document.referenceId())
                        && fixture.providerProfileId().equals(document.providerProfileId())
                        && "USD".equals(document.currencyCode()))
                .anyMatch(document -> "FINANCIAL_CLOSE_AUDIT_EVENT".equals(document.referenceType())
                        && "PERIOD_REOPENED".equals(document.status()));

        Fixture other = ownedFixture("kay022-other");
        assertThatThrownBy(() -> financialCloseService.periodDetails(financeRead(other), period.id()))
                .isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> financialCloseService.finalizedStatementDetails(financeRead(other), statement.id()))
                .isInstanceOf(NotFoundException.class);
    }

    private Fixture ownedFixture(String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.providerActorId(), fixture.slug() + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.customerActorId(), fixture.slug() + "-customer", "Customer");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.membershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", UUID.randomUUID(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'CUSTOMER')", UUID.randomUUID(), fixture.workspaceId(), fixture.customerActorId());
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", fixture.providerProfileId(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO customer_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Customer')", fixture.customerProfileId(), fixture.workspaceId(), fixture.customerActorId());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        return fixture;
    }

    private void seedSettledPayment(Fixture fixture, long gross, long fee, String reference) {
        seedOfferingAndBooking(fixture);
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', ?, ?, ?, ?, ?, ?, ?)
                """, UUID.randomUUID(), fixture.workspaceId(), fixture.bookingId(), fixture.providerProfileId(), gross, fee, gross - fee, gross, gross, gross, reference);
    }

    private UUID seedCapturedPayment(Fixture fixture, long gross, long fee, String reference) {
        UUID bookingId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, fixture.workspaceId(), fixture.offeringId(), fixture.providerProfileId(), fixture.customerProfileId(),
                java.sql.Timestamp.from(Instant.now().plusSeconds(10800)),
                java.sql.Timestamp.from(Instant.now().plusSeconds(14400)),
                java.sql.Timestamp.from(Instant.now().plusSeconds(9000)));
        UUID paymentIntentId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'CAPTURED', 'USD', ?, ?, ?, ?, ?, 0, ?)
                """, paymentIntentId, fixture.workspaceId(), bookingId, fixture.providerProfileId(), gross, fee, gross - fee, gross, gross, reference);
        return paymentIntentId;
    }

    private void seedOfferingAndBooking(Fixture fixture) {
        jdbcTemplate.update("""
                INSERT INTO offerings (id, workspace_id, provider_profile_id, title, status, duration_minutes, offer_type, min_notice_minutes, max_notice_days, slot_interval_minutes)
                VALUES (?, ?, ?, 'KAY022 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, fixture.offeringId(), fixture.workspaceId(), fixture.providerProfileId());
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, fixture.bookingId(), fixture.workspaceId(), fixture.offeringId(), fixture.providerProfileId(), fixture.customerProfileId(),
                java.sql.Timestamp.from(Instant.now().plusSeconds(3600)),
                java.sql.Timestamp.from(Instant.now().plusSeconds(7200)),
                java.sql.Timestamp.from(Instant.now().plusSeconds(1800)));
    }

    private ProviderStatementTruth truth(Fixture fixture, String sourceReference, LocalDate start, LocalDate end, ProviderStatementAmounts amounts) {
        return new ProviderStatementTruth(fixture.providerProfileId(), "USD", start, end, sourceReference, amounts, Map.of("sourceReference", sourceReference));
    }

    private ProviderStatementAmounts amounts(long gross, long fee, long net, long payout, long refund, long dispute, long subscription) {
        return new ProviderStatementAmounts(gross, fee, net, payout, refund, dispute, subscription);
    }

    private LocalDate statementStart() {
        return LocalDate.now().minusDays(1);
    }

    private LocalDate statementEnd() {
        return LocalDate.now().plusDays(1);
    }

    private AccessContext financeRead(Fixture fixture) {
        return context(fixture, Set.of("FINANCE_READ"));
    }

    private AccessContext financeWrite(Fixture fixture) {
        return context(fixture, Set.of("FINANCE_READ", "FINANCE_WRITE"));
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return context(fixture, Set.of("PAYMENT_READ", "PAYMENT_WRITE"));
    }

    private AccessContext context(Fixture fixture, Set<String> scopes) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), fixture.ownerId(), fixture.slug() + "-owner", fixture.membershipId(), "OWNER", scopes, Set.of());
    }

    private record Fixture(UUID workspaceId, String slug, UUID ownerId, UUID membershipId, UUID providerActorId, UUID customerActorId, UUID providerProfileId, UUID customerProfileId, UUID offeringId) {
        UUID bookingId() {
            return UUID.nameUUIDFromBytes((workspaceId + "-booking").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}
