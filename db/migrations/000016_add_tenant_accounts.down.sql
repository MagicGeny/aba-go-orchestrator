BEGIN;

DROP INDEX IF EXISTS idx_chat_phone_mappings_account_phone;
DROP INDEX IF EXISTS idx_campaign_targets_tenant;
DROP INDEX IF EXISTS idx_campaign_replies_tenant;
DROP INDEX IF EXISTS idx_outbox_messages_tenant;

ALTER TABLE campaign_replies
    DROP COLUMN IF EXISTS tenant_account_id,
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE outbox_messages
    DROP COLUMN IF EXISTS tenant_account_id;

ALTER TABLE campaign_targets
    DROP COLUMN IF EXISTS tenant_account_id,
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE chat_phone_mappings
    DROP COLUMN IF EXISTS tenant_account_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_phone_mappings_tenant_phone
    ON chat_phone_mappings(tenant_id, phone_normalized);

DROP INDEX IF EXISTS idx_tenant_accounts_tenant_status;

DROP TABLE IF EXISTS tenant_accounts CASCADE;

COMMIT;
