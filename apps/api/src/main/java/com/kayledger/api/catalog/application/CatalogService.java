package com.kayledger.api.catalog.application;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kayledger.api.access.application.AccessContext;
import com.kayledger.api.access.application.AccessPolicy;
import com.kayledger.api.access.model.AccessScope;
import com.kayledger.api.access.model.WorkspaceRole;
import com.kayledger.api.catalog.Offering;
import com.kayledger.api.catalog.OfferingAvailabilityWindow;
import com.kayledger.api.catalog.OfferingDetails;
import com.kayledger.api.catalog.OfferingPricingRule;
import com.kayledger.api.catalog.OfferingStore;
import com.kayledger.api.identity.ProfileStore;
import com.kayledger.api.identity.ProviderProfile;
import com.kayledger.api.shared.api.BadRequestException;
import com.kayledger.api.shared.api.ForbiddenException;
import com.kayledger.api.shared.api.NotFoundException;

@Service
public class CatalogService {

    private static final String SCHEDULED_TIME = "SCHEDULED_TIME";
    private static final String QUANTITY = "QUANTITY";

    private final OfferingStore offeringStore;
    private final ProfileStore profileStore;
    private final AccessPolicy accessPolicy;
    private final ObjectMapper objectMapper;

    public CatalogService(
            OfferingStore offeringStore,
            ProfileStore profileStore,
            AccessPolicy accessPolicy,
            ObjectMapper objectMapper) {
        this.offeringStore = offeringStore;
        this.profileStore = profileStore;
        this.accessPolicy = accessPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OfferingDetails createDraft(AccessContext context, CreateOfferingCommand command) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.PROVIDER);
        accessPolicy.requireScope(context, AccessScope.CATALOG_WRITE);
        ProviderProfile providerProfile = providerProfile(context, command.providerProfileId());
        enforceProviderOwnership(context, providerProfile);
        validateOfferShape(command.offerType(), command.durationMinutes(), command.slotIntervalMinutes(), command.quantityAvailable());
        validatePricingRules(command.pricingRules());
        validateAvailabilityWindows(command.availabilityWindows());

        Offering offering = offeringStore.createDraft(
                context.workspaceId(),
                command.providerProfileId(),
                requireText(command.title(), "title"),
                requireOfferType(command.offerType()),
                json(command.pricingMetadata()),
                command.durationMinutes(),
                command.minNoticeMinutes() == null ? 0 : command.minNoticeMinutes(),
                command.maxNoticeDays(),
                command.slotIntervalMinutes(),
                command.quantityAvailable(),
                json(command.schedulingMetadata()));
        List<OfferingPricingRule> pricingRules = createPricingRules(context.workspaceId(), offering.id(), command.pricingRules());
        List<OfferingAvailabilityWindow> availabilityWindows = createAvailabilityWindows(context.workspaceId(), offering.id(), command.availabilityWindows());
        return new OfferingDetails(offering, pricingRules, availabilityWindows);
    }

    public List<OfferingDetails> list(AccessContext context) {
        accessPolicy.requireScope(context, AccessScope.CATALOG_READ);
        return offeringStore.listDetailsForWorkspace(context.workspaceId());
    }

    @Transactional
    public OfferingDetails publish(AccessContext context, UUID offeringId) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.PROVIDER);
        accessPolicy.requireScope(context, AccessScope.CATALOG_PUBLISH);
        Offering offering = offering(context, offeringId);
        enforceProviderOwnership(context, providerProfile(context, offering.providerProfileId()));
        Offering published = offeringStore.publish(context.workspaceId(), offeringId);
        return details(context.workspaceId(), published);
    }

    @Transactional
    public OfferingDetails archive(AccessContext context, UUID offeringId) {
        accessPolicy.requireWorkspaceRole(context, WorkspaceRole.OWNER, WorkspaceRole.ADMIN, WorkspaceRole.PROVIDER);
        accessPolicy.requireScope(context, AccessScope.CATALOG_PUBLISH);
        Offering offering = offering(context, offeringId);
        enforceProviderOwnership(context, providerProfile(context, offering.providerProfileId()));
        Offering archived = offeringStore.archive(context.workspaceId(), offeringId);
        return details(context.workspaceId(), archived);
    }

    private OfferingDetails details(UUID workspaceId, Offering offering) {
        return new OfferingDetails(
                offering,
                offeringStore.listPricingRules(workspaceId, offering.id()),
                offeringStore.listAvailabilityWindows(workspaceId, offering.id()));
    }

    private Offering offering(AccessContext context, UUID offeringId) {
        return offeringStore.findForWorkspace(context.workspaceId(), offeringId)
                .orElseThrow(() -> new NotFoundException("Offering was not found."));
    }

    private ProviderProfile providerProfile(AccessContext context, UUID providerProfileId) {
        if (providerProfileId == null) {
            throw new BadRequestException("providerProfileId is required.");
        }
        return profileStore.findProvider(context.workspaceId(), providerProfileId)
                .orElseThrow(() -> new BadRequestException("providerProfileId is not valid for this workspace."));
    }

    private void enforceProviderOwnership(AccessContext context, ProviderProfile providerProfile) {
        if (WorkspaceRole.PROVIDER.equals(context.workspaceRole()) && !providerProfile.actorId().equals(context.actorId())) {
            throw new ForbiddenException("Providers can only manage their own offerings.");
        }
    }

    private List<OfferingPricingRule> createPricingRules(UUID workspaceId, UUID offeringId, List<PricingRuleCommand> pricingRules) {
        List<PricingRuleCommand> rules = pricingRules == null ? List.of() : pricingRules;
        if (rules.isEmpty()) {
            throw new BadRequestException("At least one pricing rule is required.");
        }
        return rules.stream()
                .map(rule -> offeringStore.createPricingRule(
                        workspaceId,
                        offeringId,
                        requirePricingRuleType(rule.ruleType()),
                        requireCurrency(rule.currencyCode()),
                        requireNonNegative(rule.amountMinor(), "amountMinor"),
                        blankToNull(rule.unitName()),
                        rule.sortOrder() == null ? 0 : rule.sortOrder()))
                .toList();
    }

    private List<OfferingAvailabilityWindow> createAvailabilityWindows(
            UUID workspaceId,
            UUID offeringId,
            List<AvailabilityWindowCommand> availabilityWindows) {
        List<AvailabilityWindowCommand> windows = availabilityWindows == null ? List.of() : availabilityWindows;
        return windows.stream()
                .map(window -> offeringStore.createAvailabilityWindow(
                        workspaceId,
                        offeringId,
                        requireWeekday(window.weekday()),
                        requireTime(window.startLocalTime(), "startLocalTime"),
                        requireTime(window.endLocalTime(), "endLocalTime"),
                        window.effectiveFrom(),
                        window.effectiveTo()))
                .toList();
    }

    private void validatePricingRules(List<PricingRuleCommand> pricingRules) {
        List<PricingRuleCommand> rules = pricingRules == null ? List.of() : pricingRules;
        if (rules.isEmpty()) {
            throw new BadRequestException("At least one pricing rule is required.");
        }
        rules.forEach(rule -> {
            requirePricingRuleType(rule.ruleType());
            requireCurrency(rule.currencyCode());
            requireNonNegative(rule.amountMinor(), "amountMinor");
        });
    }

    private void validateAvailabilityWindows(List<AvailabilityWindowCommand> availabilityWindows) {
        List<AvailabilityWindowCommand> windows = availabilityWindows == null ? List.of() : availabilityWindows;
        windows.forEach(window -> {
            requireWeekday(window.weekday());
            requireTime(window.startLocalTime(), "startLocalTime");
            requireTime(window.endLocalTime(), "endLocalTime");
            if (!window.startLocalTime().isBefore(window.endLocalTime())) {
                throw new BadRequestException("startLocalTime must be before endLocalTime.");
            }
            if (window.effectiveFrom() != null && window.effectiveTo() != null && window.effectiveFrom().isAfter(window.effectiveTo())) {
                throw new BadRequestException("effectiveFrom must be before effectiveTo.");
            }
        });
    }

    private void validateOfferShape(String offerType, Integer durationMinutes, Integer slotIntervalMinutes, Integer quantityAvailable) {
        String requiredOfferType = requireOfferType(offerType);
        if (SCHEDULED_TIME.equals(requiredOfferType)) {
            requirePositive(durationMinutes, "durationMinutes");
            requirePositive(slotIntervalMinutes, "slotIntervalMinutes");
            if (quantityAvailable != null) {
                throw new BadRequestException("quantityAvailable must be omitted for scheduled time offerings.");
            }
        }
        if (QUANTITY.equals(requiredOfferType)) {
            requirePositive(quantityAvailable, "quantityAvailable");
        }
    }

    private String json(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException exception) {
            throw new BadRequestException("metadata must be valid JSON.");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new BadRequestException(field + " is required.");
        }
        return value.trim();
    }

    private static String requireOfferType(String offerType) {
        String requiredOfferType = requireText(offerType, "offerType");
        if (!List.of(SCHEDULED_TIME, QUANTITY).contains(requiredOfferType)) {
            throw new BadRequestException("offerType is invalid.");
        }
        return requiredOfferType;
    }

    private static String requirePricingRuleType(String ruleType) {
        String requiredRuleType = requireText(ruleType, "ruleType");
        if (!List.of("FIXED_PRICE", "PER_UNIT").contains(requiredRuleType)) {
            throw new BadRequestException("ruleType is invalid.");
        }
        return requiredRuleType;
    }

    private static String requireCurrency(String currencyCode) {
        String requiredCurrency = requireText(currencyCode, "currencyCode");
        if (!requiredCurrency.matches("[A-Z]{3}")) {
            throw new BadRequestException("currencyCode must be a three-letter uppercase code.");
        }
        return requiredCurrency;
    }

    private static long requireNonNegative(Long value, String field) {
        if (value == null || value < 0) {
            throw new BadRequestException(field + " must be zero or greater.");
        }
        return value;
    }

    private static int requirePositive(Integer value, String field) {
        if (value == null || value <= 0) {
            throw new BadRequestException(field + " must be greater than zero.");
        }
        return value;
    }

    private static int requireWeekday(Integer weekday) {
        if (weekday == null || weekday < 1 || weekday > 7) {
            throw new BadRequestException("weekday must be between 1 and 7.");
        }
        return weekday;
    }

    private static LocalTime requireTime(LocalTime value, String field) {
        if (value == null) {
            throw new BadRequestException(field + " is required.");
        }
        return value;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public record CreateOfferingCommand(
            UUID providerProfileId,
            String title,
            String offerType,
            Map<String, Object> pricingMetadata,
            Integer durationMinutes,
            Integer minNoticeMinutes,
            Integer maxNoticeDays,
            Integer slotIntervalMinutes,
            Integer quantityAvailable,
            Map<String, Object> schedulingMetadata,
            List<PricingRuleCommand> pricingRules,
            List<AvailabilityWindowCommand> availabilityWindows) {
    }

    public record PricingRuleCommand(
            String ruleType,
            String currencyCode,
            Long amountMinor,
            String unitName,
            Integer sortOrder) {
    }

    public record AvailabilityWindowCommand(
            Integer weekday,
            LocalTime startLocalTime,
            LocalTime endLocalTime,
            LocalDate effectiveFrom,
            LocalDate effectiveTo) {
    }
}
