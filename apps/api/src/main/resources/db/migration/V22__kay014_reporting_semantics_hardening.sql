ALTER TABLE financial_provider_summaries
    RENAME COLUMN payout_requested_amount_minor TO current_payout_requested_amount_minor;

ALTER TABLE financial_provider_summaries
    RENAME COLUMN dispute_amount_minor TO active_dispute_exposure_amount_minor;

ALTER TABLE financial_provider_summaries
    RENAME COLUMN subscription_revenue_amount_minor TO settled_subscription_net_revenue_amount_minor;
