ALTER TABLE operator_search_index_state
    DROP CONSTRAINT operator_search_reference_check;

ALTER TABLE operator_search_index_state
    ADD CONSTRAINT operator_search_reference_check CHECK (reference_type IN (
        'PAYMENT_INTENT',
        'PAYMENT_ATTEMPT',
        'REFUND',
        'REFUND_ATTEMPT',
        'PAYOUT_REQUEST',
        'PAYOUT_ATTEMPT',
        'DISPUTE',
        'PROVIDER_CALLBACK',
        'RECONCILIATION_MISMATCH',
        'SUBSCRIPTION',
        'SUBSCRIPTION_CYCLE',
        'RISK_FLAG',
        'RISK_REVIEW',
        'RISK_DECISION',
        'EXPORT_JOB',
        'EXPORT_ARTIFACT',
        'PROVIDER_RECONCILIATION_RUN',
        'PROVIDER_RECONCILIATION_ITEM',
        'ACCOUNTING_PERIOD',
        'FINALIZED_PROVIDER_STATEMENT',
        'FINANCIAL_CLOSE_AUDIT_EVENT',
        'FINANCIAL_APPROVAL_REQUEST',
        'FINANCIAL_APPROVAL_DECISION',
        'FINANCE_EVIDENCE_PACK',
        'FINANCE_EVIDENCE_EXPORT',
        'MERCHANT_FINANCE_EVENT',
        'MERCHANT_FINANCE_EVENT_DELIVERY'
    ));
