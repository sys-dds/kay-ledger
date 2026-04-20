package com.kayledger.api.merchantevents.application;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MerchantFinanceDeliveryWorker {

    private final MerchantFinanceEventService merchantFinanceEventService;
    private final MerchantFinanceDeliveryProperties properties;

    public MerchantFinanceDeliveryWorker(MerchantFinanceEventService merchantFinanceEventService, MerchantFinanceDeliveryProperties properties) {
        this.merchantFinanceEventService = merchantFinanceEventService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${kay-ledger.merchant-finance.delivery.poll-delay-ms:30000}")
    public void processDueDeliveries() {
        if (properties.isScheduledEnabled()) {
            merchantFinanceEventService.processDueDeliveries();
        }
    }
}
