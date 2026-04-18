ALTER TABLE provider_configs
    ADD COLUMN signature_contract text NOT NULL DEFAULT 'RAW_BODY_HMAC_SHA256',
    ADD CONSTRAINT provider_configs_signature_contract_check CHECK (signature_contract IN ('RAW_BODY_HMAC_SHA256'));
