package com.kayledger.api.approval.application;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kay-ledger.finance.approvals")
public class FinancialApprovalProperties {

    private boolean closeRequiresApproval;
    private long largeRefundThresholdMinor = 100_000L;
    private int executionLeaseSeconds = 300;

    public boolean isCloseRequiresApproval() {
        return closeRequiresApproval;
    }

    public void setCloseRequiresApproval(boolean closeRequiresApproval) {
        this.closeRequiresApproval = closeRequiresApproval;
    }

    public long getLargeRefundThresholdMinor() {
        return largeRefundThresholdMinor;
    }

    public void setLargeRefundThresholdMinor(long largeRefundThresholdMinor) {
        this.largeRefundThresholdMinor = largeRefundThresholdMinor;
    }

    public int getExecutionLeaseSeconds() {
        return executionLeaseSeconds;
    }

    public void setExecutionLeaseSeconds(int executionLeaseSeconds) {
        this.executionLeaseSeconds = Math.max(30, executionLeaseSeconds);
    }
}
