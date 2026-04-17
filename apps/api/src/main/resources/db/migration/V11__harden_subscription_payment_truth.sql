ALTER TABLE subscription_cycles
    ADD COLUMN currency_code char(3),
    ADD COLUMN gross_amount_minor bigint,
    ADD COLUMN fee_amount_minor bigint,
    ADD COLUMN net_amount_minor bigint;

UPDATE subscription_cycles sc
SET currency_code = sp.currency_code,
    gross_amount_minor = sp.amount_minor,
    fee_amount_minor = 0,
    net_amount_minor = sp.amount_minor
FROM subscription_plans sp
WHERE sc.workspace_id = sp.workspace_id
  AND sc.plan_id = sp.id;

ALTER TABLE subscription_cycles
    ALTER COLUMN currency_code SET NOT NULL,
    ALTER COLUMN gross_amount_minor SET NOT NULL,
    ALTER COLUMN fee_amount_minor SET NOT NULL,
    ALTER COLUMN net_amount_minor SET NOT NULL,
    ADD CONSTRAINT subscription_cycles_currency_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    ADD CONSTRAINT subscription_cycles_gross_amount_check CHECK (gross_amount_minor > 0),
    ADD CONSTRAINT subscription_cycles_fee_amount_check CHECK (fee_amount_minor >= 0),
    ADD CONSTRAINT subscription_cycles_net_amount_check CHECK (net_amount_minor >= 0),
    ADD CONSTRAINT subscription_cycles_amount_math_check CHECK (gross_amount_minor = fee_amount_minor + net_amount_minor);

CREATE INDEX subscription_cycles_payment_state_idx
    ON subscription_cycles (workspace_id, payment_intent_id, status)
    WHERE payment_intent_id IS NOT NULL;
