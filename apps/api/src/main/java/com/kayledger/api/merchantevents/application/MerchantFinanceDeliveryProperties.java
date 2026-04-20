package com.kayledger.api.merchantevents.application;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kay-ledger.merchant-finance.delivery")
public class MerchantFinanceDeliveryProperties {

    private boolean scheduledEnabled = false;
    private int batchSize = 25;
    private int maxAttempts = 3;
    private long backoffSeconds = 60;
    private int responseBodyMaxChars = 2000;
    private int timeoutSeconds = 5;
    private int leaseSeconds = 60;

    public boolean isScheduledEnabled() {
        return scheduledEnabled;
    }

    public void setScheduledEnabled(boolean scheduledEnabled) {
        this.scheduledEnabled = scheduledEnabled;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = Math.max(1, batchSize);
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = Math.max(1, maxAttempts);
    }

    public long getBackoffSeconds() {
        return backoffSeconds;
    }

    public void setBackoffSeconds(long backoffSeconds) {
        this.backoffSeconds = Math.max(1, backoffSeconds);
    }

    public int getResponseBodyMaxChars() {
        return responseBodyMaxChars;
    }

    public void setResponseBodyMaxChars(int responseBodyMaxChars) {
        this.responseBodyMaxChars = Math.max(128, responseBodyMaxChars);
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = Math.max(1, timeoutSeconds);
    }

    public int getLeaseSeconds() {
        return leaseSeconds;
    }

    public void setLeaseSeconds(int leaseSeconds) {
        this.leaseSeconds = Math.max(5, leaseSeconds);
    }
}
