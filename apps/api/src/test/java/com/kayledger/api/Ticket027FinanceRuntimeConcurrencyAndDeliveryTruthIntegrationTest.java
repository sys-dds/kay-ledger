package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.kayledger.api.approval.model.FinancialApprovalRequest;
import com.kayledger.api.approval.store.FinancialApprovalStore;
import com.kayledger.api.close.application.FinancialCloseService;
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
import com.kayledger.api.merchantevents.model.MerchantFinanceEventDelivery;
import com.kayledger.api.merchantevents.store.MerchantFinanceEventStore;
import com.kayledger.api.payment.application.PaymentService;
import com.kayledger.api.shared.api.BadRequestException;

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
        "kay-ledger.merchant-finance.delivery.backoff-seconds=1",
        "kay-ledger.merchant-finance.delivery.lease-seconds=5"
})
class Ticket027FinanceRuntimeConcurrencyAndDeliveryTruthIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("kay_ledger_ticket027")
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
        registry.add("kay-ledger.async.kafka.topic", () -> "kay-ledger.ticket027.events");
        registry.add("kay-ledger.region.replication-topic", () -> "kay-ledger.ticket027.replication");
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    MerchantFinanceEventService merchantFinanceEventService;

    @Autowired
    MerchantFinanceEventStore merchantFinanceEventStore;

    @Autowired
    FinancialApprovalService financialApprovalService;

    @Autowired
    FinancialApprovalStore financialApprovalStore;

    @Autowired
    FinancialCloseService financialCloseService;

    @Autowired
    PaymentService paymentService;

    @Autowired
    InvestigationStore investigationStore;

    @Test
    void invariant_runtime_concurrency_delivery_truth_and_operator_recovery_are_workspace_safe() throws Exception {
        Fixture alpha = ownedFixture("ticket027-alpha");
        Fixture beta = ownedFixture("ticket027-beta");
        AtomicInteger alphaDeliveries = new AtomicInteger();
        AtomicInteger betaDeliveries = new AtomicInteger();
        AtomicBoolean flakyHealthy = new AtomicBoolean(false);
        List<Map<String, Object>> alphaBodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/alpha", exchange -> respond(exchange, 200, "ok", alphaDeliveries, alphaBodies));
        server.createContext("/beta", exchange -> respond(exchange, 200, "ok", betaDeliveries, List.of()));
        server.createContext("/flaky", exchange -> respond(exchange, flakyHealthy.get() ? 200 : 500, flakyHealthy.get() ? "ok" : "retry", alphaDeliveries, alphaBodies));
        server.start();
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();

        var alphaEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(alpha), new MerchantFinanceEventService.ConfigureEndpointCommand(alpha.providerProfileId(), baseUrl + "/alpha", "alpha-secret", new String[] {MerchantFinanceEventService.EVENT_FINALIZED_STATEMENT_AVAILABLE}));
        var betaEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(beta), new MerchantFinanceEventService.ConfigureEndpointCommand(beta.providerProfileId(), baseUrl + "/beta", "beta-secret", new String[] {MerchantFinanceEventService.EVENT_FINALIZED_STATEMENT_AVAILABLE}));

        UUID alphaManualSource = emit(alpha, "manual-alpha", Instant.parse("2026-01-05T10:00:00Z"));
        emit(beta, "manual-beta", Instant.parse("2026-01-05T10:00:00Z"));
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        assertThat(alphaDeliveries.get()).isEqualTo(1);
        assertThat(deliveryFor(beta, betaEndpoint.id()).deliveryStatus()).isEqualTo("PENDING");
        assertThat(betaDeliveries.get()).isZero();
        assertThat(merchantFinanceEventService.processDueDeliveries()).isEqualTo(1);
        assertThat(betaDeliveries.get()).isEqualTo(1);

        int beforeConcurrent = alphaDeliveries.get();
        emit(alpha, "concurrent-alpha", Instant.parse("2026-01-05T11:00:00Z"));
        try (var executor = Executors.newFixedThreadPool(2)) {
            List<Callable<Integer>> workers = List.of(
                    () -> merchantFinanceEventService.processDueDeliveries(),
                    () -> merchantFinanceEventService.processDueDeliveries());
            int processed = executor.invokeAll(workers).stream()
                    .mapToInt(future -> {
                        try {
                            return future.get(10, TimeUnit.SECONDS);
                        } catch (Exception exception) {
                            throw new IllegalStateException(exception);
                        }
                    })
                    .sum();
            assertThat(processed).isEqualTo(1);
        }
        assertThat(alphaDeliveries.get()).isEqualTo(beforeConcurrent + 1);

        UUID staleSource = emit(alpha, "stale-claim", Instant.parse("2026-01-05T12:00:00Z"));
        var claimed = merchantFinanceEventStore.claimDueDeliveries(alpha.workspaceId(), "test-stale-owner", 1, 2, 5);
        assertThat(claimed).hasSize(1);
        UUID staleDeliveryId = claimed.getFirst().delivery().id();
        jdbcTemplate.update("UPDATE merchant_finance_event_deliveries SET claim_expires_at = now() - interval '1 second' WHERE workspace_id = ? AND id = ?", alpha.workspaceId(), staleDeliveryId);
        assertThat(merchantFinanceEventService.runtimeSummary(financeRead(alpha)).staleClaimCount()).isEqualTo(1);
        assertThat(merchantFinanceEventService.reclaimStaleClaims(financeWrite(alpha)).affectedCount()).isEqualTo(1);
        assertThat(deliveryById(alpha, staleDeliveryId).deliveryStatus()).isEqualTo("FAILED");
        assertThat(merchantFinanceEventService.requeueFailed(financeWrite(alpha)).affectedCount()).isEqualTo(1);
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        assertThat(deliveryById(alpha, staleDeliveryId).deliveryStatus()).isEqualTo("SUCCEEDED");
        assertThat(staleSource).isNotNull();

        var flakyEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(alpha), new MerchantFinanceEventService.ConfigureEndpointCommand(alpha.providerProfileId(), baseUrl + "/flaky", "flaky-secret", new String[] {MerchantFinanceEventService.EVENT_APPROVAL_REJECTED}));
        UUID redriveSource = UUID.randomUUID();
        merchantFinanceEventService.emit(alpha.workspaceId(), alpha.providerProfileId(), "USD", null, MerchantFinanceEventService.EVENT_APPROVAL_REJECTED, "FINANCIAL_APPROVAL_REQUEST", redriveSource, Map.of("approvalRequestId", redriveSource), Instant.parse("2026-01-05T13:00:00Z"));
        long eventCountBeforeRedrive = eventCount(alpha, redriveSource);
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        MerchantFinanceEventDelivery flakyDelivery = deliveryFor(alpha, flakyEndpoint.id());
        assertThat(flakyDelivery.deliveryStatus()).isEqualTo("FAILED");
        jdbcTemplate.update("UPDATE merchant_finance_event_deliveries SET next_attempt_at = now() WHERE workspace_id = ? AND id = ?", alpha.workspaceId(), flakyDelivery.id());
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        flakyDelivery = deliveryFor(alpha, flakyEndpoint.id());
        assertThat(flakyDelivery.deliveryStatus()).isEqualTo("PARKED");
        flakyHealthy.set(true);
        merchantFinanceEventService.redriveDelivery(financeWrite(alpha), flakyDelivery.id());
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        assertThat(deliveryFor(alpha, flakyEndpoint.id()).deliveryStatus()).isEqualTo("SUCCEEDED");
        assertThat(eventCount(alpha, redriveSource)).isEqualTo(eventCountBeforeRedrive);

        Map<String, Object> envelope = alphaBodies.stream()
                .filter(body -> body.containsKey("envelopeVersion") && body.containsKey("deliveryId"))
                .findFirst()
                .orElseThrow();
        assertThat(envelope).containsKeys("envelopeVersion", "eventId", "deliveryId", "eventType", "sourceReferenceType", "sourceReferenceId", "eventKey", "occurredAt", "attemptedAt", "payload");
        assertThat(envelope.get("envelopeVersion")).isEqualTo(1);

        UUID failedPayoutId = seedPayout(alpha, "FAILED", 2500);
        var payoutApproval = approvedPayoutSuccess(alpha, failedPayoutId, 2500);
        assertThatThrownBy(() -> paymentService.markPayoutSucceeded(paymentWrite(alpha), failedPayoutId, new PaymentService.PayoutMutationCommand(null, "expected-failure", payoutApproval.id())))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Only requested or processing payouts can succeed");
        var failedHistory = financialApprovalService.history(financeRead(alpha), payoutApproval.id());
        assertThat(failedHistory.request().status()).isEqualTo("APPROVED");
        assertThat(failedHistory.executionState().executionStatus()).isEqualTo("FAILED");
        jdbcTemplate.update("UPDATE payout_requests SET status = 'REQUESTED', failure_reason = NULL WHERE workspace_id = ? AND id = ?", alpha.workspaceId(), failedPayoutId);
        Instant payoutBefore = Instant.now().minusSeconds(2);
        paymentService.markPayoutSucceeded(paymentWrite(alpha), failedPayoutId, new PaymentService.PayoutMutationCommand(null, "success-after-retry", payoutApproval.id()));
        var executedHistory = financialApprovalService.history(financeRead(alpha), payoutApproval.id());
        assertThat(executedHistory.request().status()).isEqualTo("EXECUTED");
        assertThat(executedHistory.executionState().executionStatus()).isEqualTo("EXECUTED");
        assertThatThrownBy(() -> paymentService.markPayoutSucceeded(paymentWrite(alpha), failedPayoutId, new PaymentService.PayoutMutationCommand(null, "double-exec", payoutApproval.id())))
                .isInstanceOf(BadRequestException.class);
        Instant payoutOccurredAt = jdbcTemplate.queryForObject("""
                SELECT occurred_at
                FROM merchant_finance_events
                WHERE workspace_id = ?
                  AND source_reference_type = 'PAYOUT_REQUEST'
                  AND source_reference_id = ?
                  AND event_type = ?
                """, (rs, rowNum) -> rs.getTimestamp("occurred_at").toInstant(), alpha.workspaceId(), failedPayoutId, MerchantFinanceEventService.EVENT_PAYOUT_SUCCEEDED);
        assertThat(payoutOccurredAt).isAfterOrEqualTo(payoutBefore);

        var staleApproval = financialApprovalService.createRequest(financeWrite(alpha), new CreateApprovalRequestCommand(
                FinancialApprovalService.ACTION_FINANCIAL_PERIOD_REOPEN,
                "ACCOUNTING_PERIOD",
                UUID.randomUUID(),
                null,
                null,
                null,
                "Stale execution recovery proof."));
        financialApprovalService.approve(financeWriteChecker(alpha), staleApproval.id(), "Checker approves stale recovery proof.");
        financialApprovalStore.markExecutionInProgress(alpha.workspaceId(), staleApproval.id(), alpha.ownerId(), 30, true);
        jdbcTemplate.update("UPDATE financial_approval_execution_state SET execution_lease_expires_at = now() - interval '1 second' WHERE workspace_id = ? AND approval_request_id = ?", alpha.workspaceId(), staleApproval.id());
        assertThat(financialApprovalService.executionRuntimeSummary(financeRead(alpha)).staleInProgressCount()).isEqualTo(1);
        assertThat(financialApprovalService.recoverStaleExecutions(financeWrite(alpha)).recoveredCount()).isEqualTo(1);
        assertThat(financialApprovalService.history(financeRead(alpha), staleApproval.id()).executionState().executionStatus()).isEqualTo("FAILED");

        LocalDate closedStart = LocalDate.now().minusDays(20);
        var closedPeriod = financialCloseService.openPeriod(financeWrite(alpha), closedStart, closedStart);
        financialCloseService.closePeriod(financeWrite(alpha), closedPeriod.id(), "Empty closed period for event-time block.");
        UUID providerTruthIntent = seedCreatedPayment(alpha, 8000, 800, "provider-truth-blocked");
        assertThatThrownBy(() -> paymentService.applyProviderPaymentTruth(alpha.workspaceId(), providerTruthIntent, "SETTLED", 8000, "late-provider", closedStart.atStartOfDay().plusHours(4).toInstant(java.time.ZoneOffset.UTC)))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("closed accounting period");
        Instant providerEffectiveAt = Instant.parse("2026-02-02T15:30:00Z");
        UUID openProviderTruthIntent = seedCreatedPayment(alpha, 9000, 900, "provider-truth-open");
        paymentService.applyProviderPaymentTruth(alpha.workspaceId(), openProviderTruthIntent, "SETTLED", 9000, "provider-effective", providerEffectiveAt);
        Instant storedEffectiveAt = jdbcTemplate.queryForObject("SELECT settled_effective_at FROM payment_intents WHERE workspace_id = ? AND id = ?", (rs, rowNum) -> rs.getTimestamp("settled_effective_at").toInstant(), alpha.workspaceId(), openProviderTruthIntent);
        assertThat(storedEffectiveAt).isEqualTo(providerEffectiveAt);

        assertThat(investigationStore.documentsForWorkspace(alpha.workspaceId()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT_DELIVERY".equals(document.referenceType()) && Set.of("FAILED_DELIVERY", "PARKED_DELIVERY", "STALE_CLAIMED_DELIVERY", MerchantFinanceEventService.EVENT_APPROVAL_REJECTED, MerchantFinanceEventService.EVENT_PAYOUT_SUCCEEDED).contains(document.mismatchType()))
                .anyMatch(document -> "FINANCIAL_APPROVAL_EXECUTION".equals(document.referenceType()) && Set.of("FAILED_APPROVAL_EXECUTION", "STALE_APPROVAL_EXECUTION").contains(document.mismatchType()));
        assertThat(investigationStore.documentsForWorkspace(beta.workspaceId()))
                .noneMatch(document -> alphaManualSource.equals(document.businessReferenceId()) || failedPayoutId.equals(document.businessReferenceId()));
        assertThat(merchantFinanceEventService.listDeliveries(financeRead(beta)))
                .allMatch(delivery -> beta.workspaceId().equals(delivery.workspaceId()));
        server.stop(0);
    }

    private UUID emit(Fixture fixture, String key, Instant occurredAt) {
        UUID sourceId = UUID.nameUUIDFromBytes((fixture.slug() + ":" + key).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        merchantFinanceEventService.emit(fixture.workspaceId(), fixture.providerProfileId(), "USD", null, MerchantFinanceEventService.EVENT_FINALIZED_STATEMENT_AVAILABLE, "FINALIZED_PROVIDER_STATEMENT", sourceId, Map.of("source", key), occurredAt);
        return sourceId;
    }

    private FinancialApprovalRequest approvedPayoutSuccess(Fixture fixture, UUID payoutId, long amountMinor) {
        var request = financialApprovalService.createRequest(financeWrite(fixture), new CreateApprovalRequestCommand(
                FinancialApprovalService.ACTION_PAYOUT_OPERATOR_SUCCESS,
                "PAYOUT_REQUEST",
                payoutId,
                fixture.providerProfileId(),
                "USD",
                amountMinor,
                "Operator payout success requires approval."));
        financialApprovalService.approve(financeWriteChecker(fixture), request.id(), "Approved payout success.");
        return request;
    }

    private MerchantFinanceEventDelivery deliveryFor(Fixture fixture, UUID endpointId) {
        return merchantFinanceEventService.listDeliveries(financeRead(fixture)).stream()
                .filter(delivery -> endpointId.equals(delivery.endpointId()))
                .findFirst()
                .orElseThrow();
    }

    private MerchantFinanceEventDelivery deliveryById(Fixture fixture, UUID deliveryId) {
        return merchantFinanceEventService.listDeliveries(financeRead(fixture)).stream()
                .filter(delivery -> deliveryId.equals(delivery.id()))
                .findFirst()
                .orElseThrow();
    }

    private long eventCount(Fixture fixture, UUID sourceId) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*)
                FROM merchant_finance_events
                WHERE workspace_id = ?
                  AND source_reference_id = ?
                """, Long.class, fixture.workspaceId(), sourceId);
        return count == null ? 0 : count;
    }

    private void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String responseBody, AtomicInteger counter, List<Map<String, Object>> bodies) throws IOException {
        counter.incrementAndGet();
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        if (!bodies.getClass().getName().contains("Immutable")) {
            bodies.add(readJson(requestBody));
        }
        byte[] response = responseBody.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private Map<String, Object> readJson(byte[] body) {
        try {
            return objectMapper.readValue(body, new TypeReference<>() {
            });
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
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
                VALUES (?, ?, ?, 'Ticket027 Offering', 'PUBLISHED', 60, 'SCHEDULED_TIME', 0, 30, 30)
                """, fixture.offeringId(), fixture.workspaceId(), fixture.providerProfileId());
    }

    private void seedFinancialAccounts(Fixture fixture) {
        seedFinancialAccount(fixture, "PLATFORM_CLEARING", "ASSET");
        seedFinancialAccount(fixture, "AUTHORIZED_FUNDS", "LIABILITY");
        seedFinancialAccount(fixture, "CAPTURED_FUNDS", "LIABILITY");
        seedFinancialAccount(fixture, "CASH_PLACEHOLDER", "ASSET");
        seedFinancialAccount(fixture, "SELLER_PAYABLE", "LIABILITY");
        seedFinancialAccount(fixture, "FEE_REVENUE", "REVENUE");
        seedFinancialAccount(fixture, "PAYOUT_CLEARING", "LIABILITY");
        seedFinancialAccount(fixture, "REFUND_RESERVE", "LIABILITY");
        seedFinancialAccount(fixture, "REFUND_LIABILITY", "LIABILITY");
        seedFinancialAccount(fixture, "DISPUTE_RESERVE", "LIABILITY");
    }

    private void seedFinancialAccount(Fixture fixture, String purpose, String type) {
        jdbcTemplate.update("""
                INSERT INTO financial_accounts (id, workspace_id, account_code, account_name, account_type, account_purpose, currency_code)
                VALUES (?, ?, ?, ?, ?, ?, 'USD')
                """, UUID.randomUUID(), fixture.workspaceId(), purpose + "-USD", purpose, type, purpose);
    }

    private UUID seedCreatedPayment(Fixture fixture, long gross, long fee, String reference) {
        UUID bookingId = seedBooking(fixture);
        UUID paymentIntentId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO payment_intents (
                    id, workspace_id, booking_id, provider_profile_id, status, currency_code,
                    gross_amount_minor, fee_amount_minor, net_amount_minor,
                    authorized_amount_minor, captured_amount_minor, settled_amount_minor, external_reference
                )
                VALUES (?, ?, ?, ?, 'CREATED', 'USD', ?, ?, ?, 0, 0, 0, ?)
                """, paymentIntentId, fixture.workspaceId(), bookingId, fixture.providerProfileId(), gross, fee, gross - fee, reference);
        return paymentIntentId;
    }

    private UUID seedPayout(Fixture fixture, String status, long amountMinor) {
        UUID payoutId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO payout_requests (id, workspace_id, provider_profile_id, currency_code, requested_amount_minor, status, failure_reason)
                VALUES (?, ?, ?, 'USD', ?, ?, ?)
                """, payoutId, fixture.workspaceId(), fixture.providerProfileId(), amountMinor, status, "FAILED".equals(status) ? "provider failed" : null);
        return payoutId;
    }

    private UUID seedBooking(Fixture fixture) {
        UUID bookingId = UUID.randomUUID();
        Instant startAt = Instant.now().plusSeconds(3600 + Math.abs(bookingId.getLeastSignificantBits() % 100_000));
        jdbcTemplate.update("""
                INSERT INTO bookings (id, workspace_id, offering_id, provider_profile_id, customer_profile_id, offer_type, scheduled_start_at, scheduled_end_at, quantity_reserved, hold_expires_at)
                VALUES (?, ?, ?, ?, ?, 'SCHEDULED_TIME', ?, ?, 1, ?)
                """, bookingId, fixture.workspaceId(), fixture.offeringId(), fixture.providerProfileId(), fixture.customerProfileId(),
                Timestamp.from(startAt),
                Timestamp.from(startAt.plusSeconds(3600)),
                Timestamp.from(startAt.minusSeconds(1800)));
        return bookingId;
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
