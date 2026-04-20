package com.kayledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.kayledger.api.investigation.store.InvestigationStore;
import com.kayledger.api.merchantevents.application.MerchantFinanceEventService;
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
        "kay-ledger.merchant-finance.delivery.lease-seconds=1",
        "kay-ledger.financial-approvals.execution-lease-seconds=1"
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
    MerchantFinanceEventService merchantFinanceEventService;

    @Autowired
    FinancialApprovalService financialApprovalService;

    @Autowired
    PaymentService paymentService;

    @Autowired
    InvestigationStore investigationStore;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void finance_runtime_concurrency_recovery_contract_time_and_investigation_truth_hold() throws Exception {
        Fixture alpha = ownedFixture("ticket027-alpha");
        Fixture beta = ownedFixture("ticket027-beta");
        List<String> bodies = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger hitCount = new AtomicInteger();
        AtomicInteger flakyCount = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/deliver", exchange -> {
            hitCount.incrementAndGet();
            bodies.add(new String(exchange.getRequestBody().readAllBytes()));
            byte[] response = "ok".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/flaky", exchange -> {
            flakyCount.incrementAndGet();
            bodies.add(new String(exchange.getRequestBody().readAllBytes()));
            byte[] response = "retry".getBytes();
            exchange.sendResponseHeaders(500, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();

        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        var alphaEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(alpha), new MerchantFinanceEventService.ConfigureEndpointCommand(
                alpha.providerProfileId(), baseUrl + "/deliver", "ticket027-secret", new String[0]));
        var betaEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(beta), new MerchantFinanceEventService.ConfigureEndpointCommand(
                beta.providerProfileId(), baseUrl + "/deliver", "ticket027-beta-secret", new String[0]));

        Instant providerEffectiveAt = Instant.parse("2026-01-02T03:04:05Z");
        var providerPayout = seedPayout(alpha, "REQUESTED", 4100);
        paymentService.applyProviderPayoutTruth(alpha.workspaceId(), providerPayout, "SUCCEEDED", "provider-payout", null, providerEffectiveAt);
        UUID providerDeliveryId = deliveryIdForEndpoint(alpha, alphaEndpoint.id());

        var executor = Executors.newFixedThreadPool(2);
        List<Callable<Integer>> work = List.of(
                () -> merchantFinanceEventService.processDueDeliveries(),
                () -> merchantFinanceEventService.processDueDeliveries());
        int processed = executor.invokeAll(work).stream().mapToInt(future -> {
            try {
                return future.get();
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        }).sum();
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(processed).isEqualTo(1);
        assertThat(hitCount.get()).isEqualTo(1);
        assertThat(deliveryStatus(alpha, providerDeliveryId)).isEqualTo("SUCCEEDED");

        UUID staleEventId = merchantFinanceEventService.emit(alpha.workspaceId(), alpha.providerProfileId(), "USD", null,
                MerchantFinanceEventService.EVENT_APPROVAL_REJECTED, "FINANCIAL_APPROVAL_REQUEST", UUID.randomUUID(),
                Map.of("source", "stale-claim"), Instant.parse("2026-01-03T00:00:00Z")).id();
        UUID staleDeliveryId = deliveryIdForEvent(alpha, staleEventId);
        jdbcTemplate.update("""
                UPDATE merchant_finance_event_deliveries
                SET delivery_status = 'CLAIMED', claim_owner = 'abandoned-worker', claimed_at = now() - interval '10 minutes',
                    claim_expires_at = now() - interval '5 minutes'
                WHERE workspace_id = ? AND id = ?
                """, alpha.workspaceId(), staleDeliveryId);
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        assertThat(deliveryStatus(alpha, staleDeliveryId)).isEqualTo("SUCCEEDED");

        merchantFinanceEventService.emit(alpha.workspaceId(), alpha.providerProfileId(), "USD", null,
                MerchantFinanceEventService.EVENT_APPROVAL_GRANTED, "FINANCIAL_APPROVAL_REQUEST", UUID.randomUUID(),
                Map.of("tenant", "alpha"), Instant.now());
        merchantFinanceEventService.emit(beta.workspaceId(), beta.providerProfileId(), "USD", null,
                MerchantFinanceEventService.EVENT_APPROVAL_GRANTED, "FINANCIAL_APPROVAL_REQUEST", UUID.randomUUID(),
                Map.of("tenant", "beta"), Instant.now());
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isEqualTo(1);
        assertThat(pendingCount(beta)).isEqualTo(1);
        assertThat(merchantFinanceEventService.processDueDeliveries()).isGreaterThanOrEqualTo(1);
        assertThat(pendingCount(beta)).isZero();

        UUID failedPayout = seedPayout(alpha, "FAILED", 3000);
        var failedApproval = approval(alpha, failedPayout, 3000);
        assertThatThrownBy(() -> paymentService.markPayoutSucceeded(paymentWrite(alpha), failedPayout,
                new PaymentService.PayoutMutationCommand(null, "still-failed", failedApproval.id())))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Only requested or processing payouts can succeed");
        var failedHistory = financialApprovalService.history(financeRead(alpha), failedApproval.id());
        assertThat(failedHistory.request().status()).isEqualTo("APPROVED");
        assertThat(failedHistory.executionState().executionStatus()).isEqualTo("FAILED");

        UUID stuckPayout = seedPayout(alpha, "REQUESTED", 3200);
        var stuckApproval = approval(alpha, stuckPayout, 3200);
        jdbcTemplate.update("""
                UPDATE financial_approval_execution_state
                SET execution_status = 'IN_PROGRESS', executed_by_actor_id = ?, started_at = now() - interval '10 minutes',
                    last_attempt_at = now() - interval '10 minutes', execution_attempt_count = 1,
                    execution_lease_expires_at = now() - interval '5 minutes', retryable_after_failure = true
                WHERE workspace_id = ? AND approval_request_id = ?
                """, alpha.ownerId(), alpha.workspaceId(), stuckApproval.id());
        assertThat(financialApprovalService.recoverStaleExecutions(financeWrite(alpha)).recoveredCount()).isGreaterThanOrEqualTo(1);
        Instant operatorBefore = Instant.now().minusSeconds(1);
        paymentService.markPayoutSucceeded(paymentWrite(alpha), stuckPayout,
                new PaymentService.PayoutMutationCommand(null, "recovered", stuckApproval.id()));
        Instant operatorAfter = Instant.now().plusSeconds(1);
        assertThat(financialApprovalService.history(financeRead(alpha), stuckApproval.id()).executionState().executionStatus()).isEqualTo("EXECUTED");
        Instant operatorOccurredAt = merchantEventOccurredAt(alpha, "PAYOUT_REQUEST", stuckPayout);
        assertThat(operatorOccurredAt).isBetween(operatorBefore, operatorAfter);
        assertThatThrownBy(() -> paymentService.markPayoutSucceeded(paymentWrite(alpha), stuckPayout,
                new PaymentService.PayoutMutationCommand(null, "double", stuckApproval.id())))
                .isInstanceOf(BadRequestException.class);

        var envelope = objectMapper.readValue(bodies.getFirst(), Map.class);
        assertThat(envelope).containsKeys("envelopeVersion", "eventId", "deliveryId", "eventType", "sourceReferenceType",
                "sourceReferenceId", "eventKey", "occurredAt", "attemptedAt", "payload");
        assertThat(envelope.get("occurredAt")).isEqualTo(providerEffectiveAt.toString());
        Map<?, ?> envelopePayload = (Map<?, ?>) envelope.get("payload");
        assertThat(envelopePayload.get("effectiveAt")).isEqualTo(providerEffectiveAt.toString());

        var flakyEndpoint = merchantFinanceEventService.configureEndpoint(financeWrite(alpha), new MerchantFinanceEventService.ConfigureEndpointCommand(
                alpha.providerProfileId(), baseUrl + "/flaky", "ticket027-flaky", new String[] {MerchantFinanceEventService.EVENT_APPROVAL_REJECTED}));
        UUID redriveEventId = merchantFinanceEventService.emit(alpha.workspaceId(), alpha.providerProfileId(), "USD", null,
                MerchantFinanceEventService.EVENT_APPROVAL_REJECTED, "FINANCIAL_APPROVAL_REQUEST", UUID.randomUUID(),
                Map.of("redrive", true), Instant.parse("2026-01-04T00:00:00Z")).id();
        int eventRowsBeforeRedrive = eventCount(alpha);
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isGreaterThanOrEqualTo(1);
        jdbcTemplate.update("UPDATE merchant_finance_event_deliveries SET next_attempt_at = now() WHERE workspace_id = ? AND endpoint_id = ?", alpha.workspaceId(), flakyEndpoint.id());
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isGreaterThanOrEqualTo(1);
        UUID redriveDeliveryId = deliveryIdForEventAndEndpoint(alpha, redriveEventId, flakyEndpoint.id());
        assertThat(deliveryStatus(alpha, redriveDeliveryId)).isEqualTo("PARKED");
        assertThat(investigationStore.documentsForWorkspace(alpha.workspaceId()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT_DELIVERY".equals(document.referenceType()) && redriveDeliveryId.equals(document.referenceId()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT_DELIVERY".equals(document.referenceType()) && "PARKED_DELIVERY".equals(document.mismatchType()));
        merchantFinanceEventService.redriveDelivery(financeWrite(alpha), redriveDeliveryId);
        assertThat(merchantFinanceEventService.processDueDeliveries(financeWrite(alpha))).isGreaterThanOrEqualTo(1);
        assertThat(eventCount(alpha)).isEqualTo(eventRowsBeforeRedrive);
        assertThat(flakyCount.get()).isEqualTo(3);

        assertThat(investigationStore.documentsForWorkspace(alpha.workspaceId()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT_DELIVERY".equals(document.referenceType()) && redriveDeliveryId.equals(document.referenceId()))
                .anyMatch(document -> "FINANCIAL_APPROVAL_EXECUTION".equals(document.referenceType()) && "FAILED_APPROVAL_EXECUTION".equals(document.mismatchType()))
                .anyMatch(document -> "MERCHANT_FINANCE_EVENT".equals(document.referenceType()) && redriveEventId.equals(document.referenceId()));
        assertThat(merchantFinanceEventService.listDeliveries(financeRead(beta)))
                .noneMatch(delivery -> redriveDeliveryId.equals(delivery.id()) || providerDeliveryId.equals(delivery.id()));

        server.stop(0);
    }

    private com.kayledger.api.approval.model.FinancialApprovalRequest approval(Fixture fixture, UUID payoutId, long amountMinor) {
        var approval = financialApprovalService.createRequest(financeWrite(fixture), new CreateApprovalRequestCommand(
                FinancialApprovalService.ACTION_PAYOUT_OPERATOR_SUCCESS,
                "PAYOUT_REQUEST",
                payoutId,
                fixture.providerProfileId(),
                "USD",
                amountMinor,
                "Ticket 027 approval."));
        financialApprovalService.approve(financeWriteChecker(fixture), approval.id(), "Checker approved.");
        return approval;
    }

    private UUID seedPayout(Fixture fixture, String status, long amountMinor) {
        UUID payoutId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO payout_requests (id, workspace_id, provider_profile_id, currency_code, requested_amount_minor, status, failure_reason)
                VALUES (?, ?, ?, 'USD', ?, ?, CASE WHEN ? = 'FAILED' THEN 'provider failed' ELSE NULL END)
                """, payoutId, fixture.workspaceId(), fixture.providerProfileId(), amountMinor, status, status);
        return payoutId;
    }

    private UUID deliveryIdForEndpoint(Fixture fixture, UUID endpointId) {
        return jdbcTemplate.queryForObject("""
                SELECT id FROM merchant_finance_event_deliveries
                WHERE workspace_id = ? AND endpoint_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """, UUID.class, fixture.workspaceId(), endpointId);
    }

    private UUID deliveryIdForEvent(Fixture fixture, UUID eventId) {
        return jdbcTemplate.queryForObject("""
                SELECT id FROM merchant_finance_event_deliveries
                WHERE workspace_id = ? AND merchant_finance_event_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """, UUID.class, fixture.workspaceId(), eventId);
    }

    private UUID deliveryIdForEventAndEndpoint(Fixture fixture, UUID eventId, UUID endpointId) {
        return jdbcTemplate.queryForObject("""
                SELECT id FROM merchant_finance_event_deliveries
                WHERE workspace_id = ? AND merchant_finance_event_id = ? AND endpoint_id = ?
                """, UUID.class, fixture.workspaceId(), eventId, endpointId);
    }

    private String deliveryStatus(Fixture fixture, UUID deliveryId) {
        return jdbcTemplate.queryForObject("""
                SELECT delivery_status FROM merchant_finance_event_deliveries
                WHERE workspace_id = ? AND id = ?
                """, String.class, fixture.workspaceId(), deliveryId);
    }

    private int pendingCount(Fixture fixture) {
        return jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM merchant_finance_event_deliveries
                WHERE workspace_id = ? AND delivery_status = 'PENDING'
                """, Integer.class, fixture.workspaceId());
    }

    private int eventCount(Fixture fixture) {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM merchant_finance_events WHERE workspace_id = ?", Integer.class, fixture.workspaceId());
    }

    private Instant merchantEventOccurredAt(Fixture fixture, String sourceType, UUID sourceId) {
        return jdbcTemplate.queryForObject("""
                SELECT occurred_at FROM merchant_finance_events
                WHERE workspace_id = ? AND source_reference_type = ? AND source_reference_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """, (rs, rowNum) -> rs.getTimestamp("occurred_at").toInstant(), fixture.workspaceId(), sourceType, sourceId);
    }

    private Fixture ownedFixture(String slug) {
        Fixture fixture = new Fixture(UUID.randomUUID(), slug, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        jdbcTemplate.update("INSERT INTO workspaces (id, slug, display_name) VALUES (?, ?, ?)", fixture.workspaceId(), fixture.slug(), fixture.slug());
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.ownerId(), fixture.slug() + "-owner", "Owner");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.checkerId(), fixture.slug() + "-checker", "Checker");
        jdbcTemplate.update("INSERT INTO actors (id, actor_key, display_name) VALUES (?, ?, ?)", fixture.providerActorId(), fixture.slug() + "-provider", "Provider");
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'OWNER')", fixture.ownerMembershipId(), fixture.workspaceId(), fixture.ownerId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'ADMIN')", fixture.checkerMembershipId(), fixture.workspaceId(), fixture.checkerId());
        jdbcTemplate.update("INSERT INTO workspace_memberships (id, workspace_id, actor_id, role) VALUES (?, ?, ?, 'PROVIDER')", UUID.randomUUID(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO provider_profiles (id, workspace_id, actor_id, display_name) VALUES (?, ?, ?, 'Provider')", fixture.providerProfileId(), fixture.workspaceId(), fixture.providerActorId());
        jdbcTemplate.update("INSERT INTO workspace_region_ownership (workspace_id, home_region, ownership_epoch) VALUES (?, 'region-a', 1)", fixture.workspaceId());
        seedFinancialAccounts(fixture);
        return fixture;
    }

    private void seedFinancialAccounts(Fixture fixture) {
        seedFinancialAccount(fixture, "SELLER_PAYABLE", "LIABILITY");
        seedFinancialAccount(fixture, "PAYOUT_CLEARING", "LIABILITY");
        seedFinancialAccount(fixture, "CASH_PLACEHOLDER", "ASSET");
    }

    private void seedFinancialAccount(Fixture fixture, String purpose, String type) {
        jdbcTemplate.update("""
                INSERT INTO financial_accounts (id, workspace_id, account_code, account_name, account_type, account_purpose, currency_code)
                VALUES (?, ?, ?, ?, ?, ?, 'USD')
                """, UUID.randomUUID(), fixture.workspaceId(), purpose + "-USD", purpose, type, purpose);
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
            UUID providerProfileId,
            UUID customerProfileId,
            UUID offeringId) {
    }
}
