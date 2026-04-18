package com.kayledger.api.access.model;

public final class AccessScope {

    public static final String WORKSPACE_READ = "WORKSPACE_READ";
    public static final String ACTOR_READ = "ACTOR_READ";
    public static final String MEMBERSHIP_MANAGE = "MEMBERSHIP_MANAGE";
    public static final String PROFILE_READ = "PROFILE_READ";
    public static final String PROFILE_MANAGE = "PROFILE_MANAGE";
    public static final String ACCESS_CONTEXT_READ = "ACCESS_CONTEXT_READ";
    public static final String CATALOG_READ = "CATALOG_READ";
    public static final String CATALOG_WRITE = "CATALOG_WRITE";
    public static final String CATALOG_PUBLISH = "CATALOG_PUBLISH";
    public static final String BOOKING_CREATE = "BOOKING_CREATE";
    public static final String BOOKING_READ = "BOOKING_READ";
    public static final String BOOKING_MANAGE = "BOOKING_MANAGE";
    public static final String FINANCE_READ = "FINANCE_READ";
    public static final String FINANCE_WRITE = "FINANCE_WRITE";
    public static final String PAYMENT_READ = "PAYMENT_READ";
    public static final String PAYMENT_WRITE = "PAYMENT_WRITE";
    public static final String SUBSCRIPTION_READ = "SUBSCRIPTION_READ";
    public static final String SUBSCRIPTION_WRITE = "SUBSCRIPTION_WRITE";
    public static final String SUBSCRIPTION_RENEW = "SUBSCRIPTION_RENEW";

    private AccessScope() {
    }
}
