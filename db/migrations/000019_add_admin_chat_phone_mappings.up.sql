-- Admin (tenant-admin) chat mappings are a different relation from
-- campaign/customer chat mappings: the same phone number may be a campaign
-- target AND the tenant admin phone at the same time. They therefore live in
-- their own table, keyed by the sender account (tenant_accounts.id) that owns
-- the MAX chat, so account A can never resolve account B's chat.
--
-- Campaign/customer mappings are NOT migrated into this table: chat_phone_mappings
-- keeps them (and keeps its existing semantics) unchanged.
BEGIN;

CREATE TABLE IF NOT EXISTS admin_chat_phone_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    tenant_account_id UUID NOT NULL REFERENCES tenant_accounts(id) ON DELETE CASCADE,
    phone_normalized VARCHAR(20) NOT NULL,
    chat_id TEXT NOT NULL,
    messenger_type VARCHAR(32) NOT NULL DEFAULT 'MAX',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- The same admin phone may be mapped independently for every account.
    CONSTRAINT uq_admin_chat_phone_mappings_account_phone
        UNIQUE (tenant_account_id, phone_normalized, messenger_type)
);

CREATE INDEX IF NOT EXISTS idx_admin_chat_phone_mappings_tenant
    ON admin_chat_phone_mappings(tenant_id);

COMMIT;
