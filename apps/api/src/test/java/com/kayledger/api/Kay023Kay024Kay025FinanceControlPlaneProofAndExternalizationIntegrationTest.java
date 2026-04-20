package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
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
import com.kayledger.api.approval.application.FinancialApprovalService;
import com.kayledger.api.approval.application.FinancialApprovalService.CreateApprovalRequestCommand;
import com.kayledger.api.close.application.FinancialCloseService;
import com.kayledger.api.evidence.application.FinanceEvidenceService;
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.provider.model.ProviderStatementAmounts;
import com.kayledger.api.provider.model.ProviderStatementTruth;
import com.kayledger.api.reconciliation.application.ReconciliationService;
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
        "kay-ledger.object-storage.endpoint=http://localhost:1",
        "kay-ledger.merchant-finance.delivery.max-attempts=2",
        "kay-ledger.merchant-finance.delivery.backoff-seconds=1"
})
class Kay023Kay024Kay025FinanceControlPlaneProofAndExternalizationIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_kay023_024_025")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.kay023.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.kay023.replication");
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
    FinancialApprovalService financialApprovalService;

    @Autowired
    FinanceEvidenceService financeEvidenceService;

    @Autowired
    MerchantFinanceEventService merchantFinanceEventService;

    @Autowired
    PaymentService paymentService;

    @Autowired
    InvestigationStore investigationStore;

    @Test
    void invariant_finance_control_plane_proof_and_externalization_are_durable_and_tenant_safe() throws Exception {
        Fixture fixture = ownedFixture("kay023-main");
        AtomicInteger successDeliveries = new AtomicInteger();
        AtomicInteger redriveDeliveries = new AtomicInteger();
        AtomicBoolean redriveEndpointHealthy = new AtomicBoolean(false);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/success", exchange -> {
            successDeliveries.incrementAndGet();
            byte[] response = "delivered".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/retry", exchange -> {
            int status = redriveEndpointHealthy.get() ? 200 : 500;
            redriveDeliveries.incrementAndGet();
            byte[] response = (status == 200 ? "redriven" : "retry later").getBytes();
            exchange.sendResponseHeaders(status, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        var successEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(fixture), new MerchantFinanceEventService.ConfigureEndpointCommand(
                fixture.providerProfileId(), baseUrl + "/success", "kay023-secret", new String[] {MerchantFinanceEventService.EVENT_FINALIZED_STATEMENT_AVAILABLE}));
        var retryEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(fixture), new MerchantFinanceEventService.ConfigureEndpointCommand(
                fixture.providerProfileId(), baseUrl + "/retry", "kay023-retry-secret", new String[] {MerchantFinanceEventService.EVENT_APPROVAL_REJECTED}));

        LocalDate periodStart = LocalDate.now().minusDays(10);
        LocalDate periodEnd = periodStart.plusDays(1);
        seedSettledPayment(fixture, 10000, 1000, "kay023-event-time", periodStart.atStartOfDay().plusHours(6));

        var matchedImport = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay023-matched", periodStart, periodEnd, amounts(10000, 1000, 9000, 0, 0, 0, 0)));
        var matchedRun = reconciliationService.createRunFromProviderTruth(financeWrite(fixture), matchedImport.id());
        assertThat(matchedRun.status()).isEqualTo("MATCHED");
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), matchedImport.id()).orElseThrow().status()).isEqualTo("MATCHED");

        var mismatchedImport = reconciliationService.recordProviderTruth(financeWrite(fixture), truth(fixture, "kay023-mismatched", periodStart, periodEnd, amounts(10100, 1000, 9100, 0, 0, 0, 0)));
        var mismatchedRun = reconciliationService.createRunFromProviderTruth(financeWrite(fixture), mismatchedImport.id());
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), mismatchedImport.id()).orElseThrow().status()).isEqualTo("MISMATCHED");
        List<ReconciliationItem> items = reconciliationService.listItems(financeRead(fixture), mismatchedRun.id(), true);
        assertThat(items).extracting(ReconciliationItem::mismatchType).contains("SETTLED_GROSS_MISMATCH", "NET_EARNINGS_MISMATCH");
        for (ReconciliationItem item : items) {
            reconciliationService.resolveItem(financeWrite(fixture), item.id(), "PROVIDER_CORRECTION_ACCEPTED", "Accepted after provider correction.");
        }
        assertThat(reconciliationStore.findProviderTruthImport(fixture.workspaceId(), mismatchedImport.id()).orElseThrow().status()).isEqualTo("RESOLVED");

        var closePeriod = financialCloseService.openPeriod(financeWrite(fixture), periodStart, periodEnd);
        var close = financialCloseService.closePeriod(financeWrite(fixture), closePeriod.id(), "Evidence close.");
        assertThat(close.finalizedStatements()).hasSize(1);
        assertThat(close.finalizedStatements().getFirst().settledGrossAmountMinor()).isEqualTo(10000);
        assertThat(merchantFinanceEventService.processDueDeliveries()).isGreaterThanOrEqualTo(1);
        assertThat(successDeliveries.get()).isEqualTo(1);

        var emptyClosedPeriod = financialCloseService.openPeriod(financeWrite(fixture), LocalDate.now(), LocalDate.now());
        financialCloseService.closePeriod(financeWrite(fixture), emptyClosedPeriod.id(), "Empty close blocks the accounting window.");
        UUID capturedIntentId = seedCapturedPayment(fixture, 5000, 500, "kay023-blocked");
        assertThatThrownBy(() -> paymentService.settle(paymentWrite(fixture), capturedIntentId, new PaymentService.AmountCommand(5000L, "blocked")))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("closed accounting period");

        var reopenApproval = financialApprovalService.createRequest(financeWrite(fixture), new CreateApprovalRequestCommand(
                FinancialApprovalService.ACTION_FINANCIAL_PERIOD_REOPEN,
                "ACCOUNTING_PERIOD",
                emptyClosedPeriod.id(),
                null,
                null,
                null,
                "Provider submitted a late correction."));
        assertThatThrownBy(() -> financialCloseService.reopenPeriod(financeWrite(fixture), emptyClosedPeriod.id(), "No approval yet.", null))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("requires an approved");
        assertThatThrownBy(() -> financialApprovalService.approve(financeWrite(fixture), reopenApproval.id(), "Self approval is blocked."))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Maker and checker");
        financialApprovalService.approve(financeWriteChecker(fixture), reopenApproval.id(), "Checker approved late correction.");
        financialCloseService.reopenPeriod(financeWrite(fixture), emptyClosedPeriod.id(), "Late correction approved.", reopenApproval.id());
        var reopenHistory = financialApprovalService.history(financeRead(fixture), reopenApproval.id());
        assertThat(reopenHistory.decisions()).hasSize(1);
        assertThat(reopenHistory.executionState().executionStatus()).isEqualTo("EXECUTED");

        UUID failedPayoutId = seedFailedPayout(fixture, 3000);
        var payoutApproval = financialApprovalService.createRequest(financeWrite(fixture), new CreateApprovalRequestCommand(
                FinancialApprovalService.ACTION_PAYOUT_OPERATOR_SUCCESS,
                "PAYOUT_REQUEST",
                failedPayoutId,
                fixture.providerProfileId(),
                "USD",
                3000L,
                "Operator payout override requires approval."));
        financialApprovalService.approve(financeWriteChecker(fixture), payoutApproval.id(), "Payout override approved.");
        assertThatThrownBy(() -> paymentService.markPayoutSucceeded(paymentWrite(fixture), failedPayoutId, new PaymentService.PayoutMutationCommand(null, "failed-exec", payoutApproval.id())))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Only requested or processing payouts can succeed");
        var failedExecution = financialApprovalService.history(financeRead(fixture), payoutApproval.id());
        assertThat(failedExecution.request().status()).isEqualTo("APPROVED");
        assertThat(failedExecution.executionState().executionStatus()).isEqualTo("FAILED");
        jdbcTemplate.update("UPDATE payout_requests SET status = 'REQUESTED', failure_reason = NULL WHERE workspace_id = ? AND id = ?", fixture.workspaceId(), failedPayoutId);
        paymentService.markPayoutSucceeded(paymentWrite(fixture), failedPayoutId, new PaymentService.PayoutMutationCommand(null, "retry-exec", payoutApproval.id()));
        var successfulRetryExecution = financialApprovalService.history(financeRead(fixture), payoutApproval.id());
        assertThat(successfulRetryExecution.request().status()).isEqualTo("EXECUTED");
        assertThat(successfulRetryExecution.executionState().executionStatus()).isEqualTo("EXECUTED");

        var evidencePack = financeEvidenceService.generatePack(financeWrite(fixture), new FinanceEvidenceService.GenerateEvidencePackCommand(
                FinanceEvidenceService.TYPE_FINALIZED_PROVIDER_STATEMENT,
                close.finalizedStatements().getFirst().id()));
        var export = financeEvidenceService.generateExport(financeWrite(fixture), evidencePack.id(), "JSON");
        var secondExport = financeEvidenceService.generateExport(financeWrite(fixture), evidencePack.id(), "JSON");
        var artifact = financeEvidenceService.exportArtifact(financeRead(fixture), export.id());
        var secondArtifact = financeEvidenceService.exportArtifact(financeRead(fixture), secondExport.id());
        assertThat(export.checksumAlgorithm()).isEqualTo("SHA-256");
        assertThat(export.checksumValue()).hasSize(64);
        assertThat(secondExport.id()).isNotEqualTo(export.id());
        assertThat(secondExport.artifactId()).isNotEqualTo(export.artifactId());
        assertThat(artifact.id()).isEqualTo(export.artifactId());
        assertThat(artifact.checksumValue()).isEqualTo(sha256(artifact.artifactBody()));
        assertThat(secondArtifact.id()).isEqualTo(secondExport.artifactId());

        UUID rejectedSourceId = UUID.randomUUID();
        merchantFinanceEventService.emit(fixture.workspaceId(), fixture.providerProfileId(), "USD", null, MerchantFinanceEventService.EVENT_APPROVAL_REJECTED, "FINANCIAL_APPROVAL_REQUEST", rejectedSourceId, Map.of("approvalRequestId", rejectedSourceId), Instant.now().minusSeconds(60));
        assertThat(merchantFinanceEventService.processDueDeliveries()).isGreaterThanOrEqualTo(1);
        assertThat(deliveryForEndpoint(fixture, retryEndpoint.id()).deliveryStatus()).isEqualTo("FAILED");
        jdbcTemplate.update("UPDATE merchant_finance_event_deliveries SET next_attempt_at = now() WHERE workspace_id = ? AND endpoint_id = ?", fixture.workspaceId(), retryEndpoint.id());
        assertThat(merchantFinanceEventService.processDueDeliveries()).isGreaterThanOrEqualTo(1);
        assertThat(deliveryForEndpoint(fixture, retryEndpoint.id()).deliveryStatus()).isEqualTo("PARKED");
        redriveEndpointHealthy.set(true);
        merchantFinanceEventService.redriveDelivery(financeWrite(fixture), deliveryForEndpoint(fixture, retryEndpoint.id()).id());
        assertThat(merchantFinanceEventService.processDueDeliveries()).isGreaterThanOrEqualTo(1);
        var redrivenDelivery = deliveryForEndpoint(fixture, retryEndpoint.id());
        assertThat(redrivenDelivery.deliveryStatus()).isEqualTo("SUCCEEDED");
        assertThat(redrivenDelivery.attemptCount()).isEqualTo(3);
        assertThat(redriveDeliveries.get()).isEqualTo(3);

        var deliveries = merchantFinanceEventService.listDeliveries(financeRead(fixture));
        assertThat(deliveries).isNotEmpty();
        assertThat(deliveryForEndpoint(fixture, successEndpoint.id()).deliveryStatus()).isEqualTo("SUCCEEDED");

        assertThat(investigationStore.documentsForWorkspace(fixture.workspaceId()))
                .anyMatch(document -> "FINANCIAL_APPROVAL_REQUEST".equals(document.referenceType()) && reopenApproval.id().equals(document.referenceId()))
                .anyMatch(document -> "FINANCE_EVIDENCE_PACK".equals(document.referenceType()) && evidencePack.id().equals(document.referenceId()))
                .anyMatch(document -> "FINANCE_EVIDENCE_EXPORT".equals(document.referenceType()) && export.id().equals(document.referenceId()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT".equals(document.referenceType()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT_DELIVERY".equals(document.referenceType()));

        Fixture other = ownedFixture("kay023-other");
        assertThatThrownBy(() -> financeEvidenceService.packDetails(financeRead(other), evidencePack.id()))
                .isInstanceOf(NotFoundException.class);
        assertThat(investigationStore.documentsForWorkspace(other.workspaceId()))
                .noneMatch(document -> evidencePack.id().equals(document.referenceId()) || reopenApproval.id().equals(document.referenceId()));
        server.stop(0);
    }

    private Fixture ownedFixture(String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.checkerId(), fixture.slug() + "-checker", "Checker");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.providerActorId(), fixture.slug() + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.customerActorId(), fixture.slug() + "-customer", "Customer");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.ownerMembershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'ADMIN')", fixture.checkerMembershipId(), fixture.workspaceId(), fixture.checkerId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", UUID.randomUUID(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'CUSTOMER')", UUID.randomUUID(), fixture.workspaceId(), fixture.customerActorId());
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", fixture.providerProfileId(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO customer_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Customer')", fixture.customerProfileId(), fixture.workspaceId(), fixture.customerActorId());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        seedOffering(fixture);
        seedFinancialAccounts(fixture);
        return fixture;
    }

    private void seedOffering(Fixture fixture) {
        jdbcTemplate.update("""
                INSERT INTO offerings (id, workspace_id, provider_profile_id, title, status, duration_minutes, offer_type, min_notice_minutes, max_notice_days, slot_interval_minutes)
                VALUES (?, ?, ?, 'KAY023 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, fixture.offeringId(), fixture.workspaceId(), fixture.providerProfileId());
    }

    private void seedFinancialAccounts(Fixture fixture) {
        seedFinancialAccount(fixture, "PAYOUT_CLEARING", "LIABILITY");
        seedFinancialAccount(fixture, "CASH_PLACEHOLDER", "ASSET");
    }

    private void seedFinancialAccount(Fixture fixture, String purpose, String type) {
        jdbcTemplate.update("""
                INSERT INTO financial_accounts (id, workspace_id, account_code, account_name, account_type, account_purpose, currency_code)
                VALUES (?, ?, ?, ?, ?, ?, 'USD')
                """, UUID.randomUUID(), fixture.workspaceId(), purpose + "-USD", purpose, type, purpose);
    }

    private void seedSettledPayment(Fixture fixture, long gross, long fee, String reference, LocalDateTime effectiveAt) {
        UUID bookingId = seedBooking(fixture);
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference, settled_effective_at
                )
                VALUES (?, ?, ?, ?, 'SETTLED', 'USD', ?, ?, ?, ?, ?, ?, ?, ?)
                """, UUID.randomUUID(), fixture.workspaceId(), bookingId, fixture.providerProfileId(), gross, fee, gross - fee, gross, gross, gross, reference, Timestamp.valueOf(effectiveAt));
    }

    private UUID seedCapturedPayment(Fixture fixture, long gross, long fee, String reference) {
        UUID bookingId = seedBooking(fixture);
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

    private UUID seedFailedPayout(Fixture fixture, long amountMinor) {
        UUID payoutId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO payout_requests (id, workspace_id, provider_profile_id, currency_code, requested_amount_minor, status, failure_reason)
                VALUES (?, ?, ?, 'USD', ?, 'FAILED', 'provider failed')
                """, payoutId, fixture.workspaceId(), fixture.providerProfileId(), amountMinor);
        return payoutId;
    }

    private com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery deliveryForEndpoint(Fixture fixture, UUID endpointId) {
        return merchantFinanceEventService.listDeliveries(financeRead(fixture)).stream()
                .filter(delivery -> endpointId.equals(delivery.endpointId()))
                .findFirst()
                .orElseThrow();
    }

    private static String sha256(String body) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes()));
    }

    private UUID seedBooking(Fixture fixture) {
        UUID bookingId = UUID.randomUUID();
        long offsetSeconds = 3600 + Math.abs(bookingId.getMostSignificantBits() % 100_000);
        Instant startAt = Instant.now().plusSeconds(offsetSeconds);
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, fixture.workspaceId(), fixture.offeringId(), fixture.providerProfileId(), fixture.customerProfileId(),
                Timestamp.from(startAt),
                Timestamp.from(startAt.plusSeconds(3600)),
                Timestamp.from(startAt.minusSeconds(1800)));
        return bookingId;
    }

    private ProviderStatementTruth truth(Fixture fixture, String sourceReference, LocalDate start, LocalDate end, ProviderStatementAmounts amounts) {
        return new ProviderStatementTruth(fixture.providerProfileId(), "USD", start, end, sourceReference, amounts, Map.of("sourceReference", sourceReference));
    }

    private ProviderStatementAmounts amounts(long gross, long fee, long net, long payout, long refund, long dispute, long subscription) {
        return new ProviderStatementAmounts(gross, fee, net, payout, refund, dispute, subscription);
    }

    private AccessContext financeRead(Fixture fixture) {
        return context(fixture, fixture.ownerId(), fixture.ownerMembershipId(), fixture.slug() + "-owner", "OWNER", Set.of("FINANCE_READ"));
    }

    private AccessContext financeWrite(Fixture fixture) {
        return context(fixture, fixture.ownerId(), fixture.ownerMembershipId(), fixture.slug() + "-owner", "OWNER", Set.of("FINANCE_READ", "FINANCE_WRITE"));
    }

    private AccessContext financeWriteChecker(Fixture fixture) {
        return context(fixture, fixture.checkerId(), fixture.checkerMembershipId(), fixture.slug() + "-checker", "ADMIN", Set.of("FINANCE_READ", "FINANCE_WRITE"));
    }

    private AccessContext paymentWrite(Fixture fixture) {
        return context(fixture, fixture.ownerId(), fixture.ownerMembershipId(), fixture.slug() + "-owner", "OWNER", Set.of("PAYMENT_READ", "PAYMENT_WRITE"));
    }

    private AccessContext context(Fixture fixture, UUID actorId, UUID membershipId, String actorKey, String role, Set<String> scopes) {
        return new AccessContext(fixture.workspaceId(), fixture.slug(), actorId, actorKey, membershipId, role, scopes, Set.of());
    }

    private record Fixture(
            UUID workspaceId,
            String slug,
            UUID ownerId,
            UUID checkerId,
            UUID ownerMembershipId,
            UUID checkerMembershipId,
            UUID providerActorId,
            UUID customerActorId,
            UUID providerProfileId,
            UUID customerProfileId,
            UUID offeringId) {
    }
}
