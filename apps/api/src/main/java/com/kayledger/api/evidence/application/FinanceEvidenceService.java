package com.kayledger.api.evidence.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.approval.store.FinancialApprovalStore;
import com.kayledger.api.close.store.FinancialCloseStore;
import com.kayledger.api.evidence.model.FinanceEvidenceExport;
import com.kayledger.api.evidence.model.FinanceEvidencePack;
import com.kayledger.api.evidence.model.FinanceEvidencePackItem;
import com.kayledger.api.evidence.store.FinanceEvidenceStore;
import com.kayledger.api.reconciliation.store.ReconciliationStore;
import com.kayledger.api.region.application.RegionService;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class FinanceEvidenceService {

    public static final String TYPE_FINALIZED_PROVIDER_STATEMENT = "FINALIZED_PROVIDER_STATEMENT";
    public static final String TYPE_ACCOUNTING_PERIOD_CLOSE_HISTORY = "ACCOUNTING_PERIOD_CLOSE_HISTORY";
    public static final String TYPE_PROVIDER_RECONCILIATION = "PROVIDER_RECONCILIATION";
    public static final String TYPE_FINANCIAL_APPROVAL_CHAIN = "FINANCIAL_APPROVAL_CHAIN";

    private final FinanceEvidenceStore financeEvidenceStore;
    private final FinancialCloseStore financialCloseStore;
    private final ReconciliationStore reconciliationStore;
    private final FinancialApprovalStore financialApprovalStore;
    private final AccessPolicy accessPolicy;
    private final RegionService regionService;
    private final ObjectMapper objectMapper;

    public FinanceEvidenceService(
            FinanceEvidenceStore financeEvidenceStore,
            FinancialCloseStore financialCloseStore,
            ReconciliationStore reconciliationStore,
            FinancialApprovalStore financialApprovalStore,
            AccessPolicy accessPolicy,
            RegionService regionService,
            ObjectMapper objectMapper) {
        this.financeEvidenceStore = financeEvidenceStore;
        this.financialCloseStore = financialCloseStore;
        this.reconciliationStore = reconciliationStore;
        this.financialApprovalStore = financialApprovalStore;
        this.accessPolicy = accessPolicy;
        this.regionService = regionService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public FinanceEvidencePack generatePack(AccessContext context, GenerateEvidencePackCommand command) {
        requireWrite(context, "finance evidence pack generation");
        if (command == null) {
            throw new BadRequestException("evidence pack request body is required.");
        }
        String packType = requireText(command.evidencePackType(), "evidencePackType");
        UUID sourceId = requireId(command.sourceReferenceId(), "sourceReferenceId");
        return switch (packType) {
            case TYPE_FINALIZED_PROVIDER_STATEMENT -> finalizedStatementPack(context, sourceId);
            case TYPE_ACCOUNTING_PERIOD_CLOSE_HISTORY -> periodCloseHistoryPack(context, sourceId);
            case TYPE_PROVIDER_RECONCILIATION -> providerReconciliationPack(context, sourceId);
            case TYPE_FINANCIAL_APPROVAL_CHAIN -> approvalChainPack(context, sourceId);
            default -> throw new BadRequestException("Unsupported finance evidence pack type.");
        };
    }

    public List<FinanceEvidencePack> listPacks(AccessContext context) {
        requireRead(context);
        return financeEvidenceStore.listPacks(context.workspaceId());
    }

    public List<FinanceEvidencePack> listPacksForSource(AccessContext context, String sourceReferenceType, UUID sourceReferenceId) {
        requireRead(context);
        return financeEvidenceStore.listPacksBySource(
                context.workspaceId(),
                requireText(sourceReferenceType, "sourceReferenceType") + ":" + requireId(sourceReferenceId, "sourceReferenceId"));
    }

    public EvidencePackDetails packDetails(AccessContext context, UUID packId) {
        requireRead(context);
        FinanceEvidencePack pack = financeEvidenceStore.findPack(context.workspaceId(), requireId(packId, "packId"))
                .orElseThrow(() -> new NotFoundException("Finance evidence pack was not found."));
        return new EvidencePackDetails(pack, financeEvidenceStore.listItems(context.workspaceId(), pack.id()), financeEvidenceStore.listExports(context.workspaceId(), pack.id()));
    }

    @Transactional
    public FinanceEvidenceExport generateExport(AccessContext context, UUID packId, String artifactFormat) {
        requireWrite(context, "finance evidence export generation");
        FinanceEvidencePack pack = financeEvidenceStore.findPack(context.workspaceId(), requireId(packId, "packId"))
                .orElseThrow(() -> new NotFoundException("Finance evidence pack was not found."));
        String format = artifactFormat == null || artifactFormat.isBlank() ? "JSON" : artifactFormat.trim().toUpperCase();
        if (!"JSON".equals(format) && !"CSV".equals(format)) {
            throw new BadRequestException("artifactFormat must be JSON or CSV.");
        }
        String artifactBody = "CSV".equals(format) ? csvArtifact(pack) : jsonArtifact(pack, financeEvidenceStore.listItems(context.workspaceId(), pack.id()));
        String checksum = sha256(artifactBody);
        return financeEvidenceStore.createExport(
                context.workspaceId(),
                pack.id(),
                format,
                "finance-evidence://" + pack.id() + "/" + format.toLowerCase(),
                artifactBody.getBytes(StandardCharsets.UTF_8).length,
                "SHA-256",
                checksum,
                context.actorId());
    }

    public List<FinanceEvidenceExport> listExports(AccessContext context, UUID packId) {
        requireRead(context);
        FinanceEvidencePack pack = financeEvidenceStore.findPack(context.workspaceId(), requireId(packId, "packId"))
                .orElseThrow(() -> new NotFoundException("Finance evidence pack was not found."));
        return financeEvidenceStore.listExports(context.workspaceId(), pack.id());
    }

    private FinanceEvidencePack finalizedStatementPack(AccessContext context, UUID statementId) {
        var statement = financialCloseStore.findFinalizedStatement(context.workspaceId(), statementId)
                .orElseThrow(() -> new NotFoundException("Finalized provider statement was not found."));
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("finalizedStatement", statement);
        FinanceEvidencePack pack = financeEvidenceStore.createPack(
                context.workspaceId(),
                TYPE_FINALIZED_PROVIDER_STATEMENT,
                statement.accountingPeriodId(),
                statement.id(),
                null,
                null,
                null,
                "FINALIZED_PROVIDER_STATEMENT:" + statement.id(),
                context.actorId(),
                json(snapshot));
        financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "FINALIZED_PROVIDER_STATEMENT", statement.id(), statement.currencyCode(), json(statement));
        return pack;
    }

    private FinanceEvidencePack periodCloseHistoryPack(AccessContext context, UUID periodId) {
        var period = financialCloseStore.findPeriod(context.workspaceId(), periodId)
                .orElseThrow(() -> new NotFoundException("Accounting period was not found."));
        var audits = financialCloseStore.listAuditEvents(context.workspaceId(), period.id());
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("accountingPeriod", period);
        snapshot.put("closeAuditEvents", audits);
        FinanceEvidencePack pack = financeEvidenceStore.createPack(
                context.workspaceId(),
                TYPE_ACCOUNTING_PERIOD_CLOSE_HISTORY,
                period.id(),
                null,
                null,
                null,
                null,
                "ACCOUNTING_PERIOD:" + period.id(),
                context.actorId(),
                json(snapshot));
        financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "ACCOUNTING_PERIOD", period.id(), period.status(), json(period));
        for (var audit : audits) {
            financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "FINANCIAL_CLOSE_AUDIT_EVENT", audit.id(), audit.eventType(), json(audit));
        }
        return pack;
    }

    private FinanceEvidencePack providerReconciliationPack(AccessContext context, UUID runId) {
        var run = reconciliationStore.findRun(context.workspaceId(), runId)
                .orElseThrow(() -> new NotFoundException("Provider reconciliation run was not found."));
        var truthImport = reconciliationStore.findProviderTruthImport(context.workspaceId(), run.truthImportId()).orElse(null);
        var truthSnapshot = reconciliationStore.findProviderTruthSnapshot(context.workspaceId(), run.truthImportId()).orElse(null);
        var items = reconciliationStore.listItems(context.workspaceId(), run.id(), false);
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("reconciliationRun", run);
        snapshot.put("providerTruthImport", truthImport);
        snapshot.put("providerTruthSnapshot", truthSnapshot);
        snapshot.put("items", items);
        FinanceEvidencePack pack = financeEvidenceStore.createPack(
                context.workspaceId(),
                TYPE_PROVIDER_RECONCILIATION,
                null,
                null,
                run.id(),
                run.truthImportId(),
                null,
                "PROVIDER_RECONCILIATION_RUN:" + run.id(),
                context.actorId(),
                json(snapshot));
        financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "PROVIDER_RECONCILIATION_RUN", run.id(), run.status(), json(run));
        if (truthImport != null) {
            financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "PROVIDER_TRUTH_IMPORT", truthImport.id(), truthImport.status(), json(truthImport));
        }
        for (var item : items) {
            financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "PROVIDER_RECONCILIATION_ITEM", item.id(), item.mismatchType(), json(item));
        }
        return pack;
    }

    private FinanceEvidencePack approvalChainPack(AccessContext context, UUID approvalRequestId) {
        var request = financialApprovalStore.findRequest(context.workspaceId(), approvalRequestId)
                .orElseThrow(() -> new NotFoundException("Financial approval request was not found."));
        var decisions = financialApprovalStore.listDecisions(context.workspaceId(), request.id());
        var execution = financialApprovalStore.executionState(context.workspaceId(), request.id()).orElse(null);
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("approvalRequest", request);
        snapshot.put("approvalDecisions", decisions);
        snapshot.put("executionState", execution);
        FinanceEvidencePack pack = financeEvidenceStore.createPack(
                context.workspaceId(),
                TYPE_FINANCIAL_APPROVAL_CHAIN,
                null,
                null,
                null,
                null,
                request.id(),
                "FINANCIAL_APPROVAL_REQUEST:" + request.id(),
                context.actorId(),
                json(snapshot));
        financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "FINANCIAL_APPROVAL_REQUEST", request.id(), request.status(), json(request));
        for (var decision : decisions) {
            financeEvidenceStore.createItem(context.workspaceId(), pack.id(), "FINANCIAL_APPROVAL_DECISION", decision.id(), decision.decision(), json(decision));
        }
        return pack;
    }

    private String jsonArtifact(FinanceEvidencePack pack, List<FinanceEvidencePackItem> items) {
        Map<String, Object> artifact = new LinkedHashMap<>();
        artifact.put("pack", pack);
        artifact.put("items", items);
        return json(artifact);
    }

    private String csvArtifact(FinanceEvidencePack pack) {
        return "evidence_pack_id,evidence_pack_type,source_reference,status\n"
                + pack.id() + "," + pack.evidencePackType() + "," + pack.sourceReference() + "," + pack.status() + "\n";
    }

    private void requireRead(AccessContext context) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_READ);
    }

    private void requireWrite(AccessContext context, String operation) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN);
        accessPolicy.requireScope(context, AccessScope.FINANCE_WRITE);
        regionService.requireOwnedForWrite(context, operation);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new BadRequestException("Finance evidence snapshot could not be serialized.");
        }
    }

    private static String sha256(String artifactBody) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(artifactBody.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) {
            throw new BadRequestException("Finance evidence export checksum could not be generated.");
        }
    }

    private static UUID requireId(UUID value, String fieldName) {
        if (value == null) {
            throw new BadRequestException(fieldName + " is required.");
        }
        return value;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(fieldName + " is required.");
        }
        return value.trim();
    }

    public record GenerateEvidencePackCommand(String evidencePackType, UUID sourceReferenceId) {
    }

    public record EvidencePackDetails(FinanceEvidencePack pack, List<FinanceEvidencePackItem> items, List<FinanceEvidenceExport> exports) {
    }
}
