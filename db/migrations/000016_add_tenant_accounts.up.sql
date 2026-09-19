BEGIN;

-- 1. Tenant sender accounts (one or more MAX sessions per tenant)
CREATE TABLE IF NOT EXISTS tenant_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    account_key VARCHAR(64) NOT NULL,
    phone_number VARCHAR(64) NOT NULL DEFAULT '',
    proxy_url VARCHAR(255) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    daily_limit INT NOT NULL DEFAULT 50,
    cooldown_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_accounts_tenant_key UNIQUE (tenant_id, account_key)
);

CREATE INDEX IF NOT EXISTS idx_tenant_accounts_tenant_status
    ON tenant_accounts(tenant_id, status);

-- 2. Default legacy account per existing tenant (reuse admin_phone when present; no fabricated numbers)
INSERT INTO tenant_accounts (tenant_id, account_key, phone_number, status)
SELECT
    t.id,
    t.id::text,
    COALESCE(NULLIF(TRIM(t.admin_phone), ''), ''),
    'active'
FROM tenants t
WHERE NOT EXISTS (
    SELECT 1 FROM tenant_accounts ta WHERE ta.tenant_id = t.id
);

-- 3. Account / tenant columns on existing tables
ALTER TABLE chat_phone_mappings
    ADD COLUMN IF NOT EXISTS tenant_account_id UUID REFERENCES tenant_accounts(id) ON DELETE RESTRICT;

ALTER TABLE campaign_targets
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS tenant_account_id UUID REFERENCES tenant_accounts(id) ON DELETE SET NULL;

ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS tenant_account_id UUID REFERENCES tenant_accounts(id) ON DELETE CASCADE;

ALTER TABLE campaign_replies
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS tenant_account_id UUID REFERENCES tenant_accounts(id) ON DELETE CASCADE;

-- Ensure outbox.tenant_id exists (may already be present from 000012)
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE;

-- 4. Backfill tenant_id
UPDATE campaign_targets ct
SET tenant_id = c.tenant_id
FROM campaigns c
WHERE ct.campaign_id = c.id AND ct.tenant_id IS NULL;

UPDATE campaign_replies cr
SET tenant_id = ct.tenant_id
FROM campaign_targets ct
WHERE cr.campaign_target_id = ct.id AND cr.tenant_id IS NULL;

UPDATE outbox_messages om
SET tenant_id = (om.payload::jsonb->>'tenant_id')::uuid
WHERE om.tenant_id IS NULL
  AND (om.payload::jsonb->>'tenant_id') IS NOT NULL
  AND (om.payload::jsonb->>'tenant_id') ~* '^[0-9a-f-]{36}$';

-- 5. Backfill tenant_account_id to the tenant's legacy/default account
UPDATE chat_phone_mappings cpm
SET tenant_account_id = ta.id
FROM tenant_accounts ta
WHERE cpm.tenant_id = ta.tenant_id
  AND cpm.tenant_account_id IS NULL
  AND ta.account_key = ta.tenant_id::text;

UPDATE campaign_targets ct
SET tenant_account_id = ta.id
FROM tenant_accounts ta
WHERE ct.tenant_id = ta.tenant_id
  AND ct.tenant_account_id IS NULL
  AND ta.account_key = ta.tenant_id::text;

UPDATE outbox_messages om
SET tenant_account_id = ta.id
FROM tenant_accounts ta
WHERE om.tenant_id = ta.tenant_id
  AND om.tenant_account_id IS NULL
  AND ta.account_key = ta.tenant_id::text;

UPDATE campaign_replies cr
SET tenant_account_id = ta.id
FROM tenant_accounts ta
WHERE cr.tenant_id = ta.tenant_id
  AND cr.tenant_account_id IS NULL
  AND ta.account_key = ta.tenant_id::text;

-- Fallback: any remaining NULLs → first account of that tenant
UPDATE chat_phone_mappings cpm
SET tenant_account_id = (
    SELECT ta.id FROM tenant_accounts ta
    WHERE ta.tenant_id = cpm.tenant_id
    ORDER BY ta.created_at ASC, ta.id ASC
    LIMIT 1
)
WHERE cpm.tenant_account_id IS NULL;

UPDATE outbox_messages om
SET tenant_account_id = (
    SELECT ta.id FROM tenant_accounts ta
    WHERE ta.tenant_id = om.tenant_id
    ORDER BY ta.created_at ASC, ta.id ASC
    LIMIT 1
)
WHERE om.tenant_account_id IS NULL AND om.tenant_id IS NOT NULL;

UPDATE campaign_replies cr
SET tenant_account_id = (
    SELECT ta.id FROM tenant_accounts ta
    WHERE ta.tenant_id = cr.tenant_id
    ORDER BY ta.created_at ASC, ta.id ASC
    LIMIT 1
)
WHERE cr.tenant_account_id IS NULL AND cr.tenant_id IS NOT NULL;

-- 6. NOT NULL where safe (campaign_targets.tenant_account_id stays nullable until dosing assigns it)
ALTER TABLE campaign_targets
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE campaign_replies
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE chat_phone_mappings
    ALTER COLUMN tenant_account_id SET NOT NULL;

ALTER TABLE outbox_messages
    ALTER COLUMN tenant_account_id SET NOT NULL;

ALTER TABLE campaign_replies
    ALTER COLUMN tenant_account_id SET NOT NULL;

-- 7. Indexes
CREATE INDEX IF NOT EXISTS idx_campaign_targets_tenant
    ON campaign_targets(tenant_id);

CREATE INDEX IF NOT EXISTS idx_campaign_replies_tenant
    ON campaign_replies(tenant_id);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_tenant
    ON outbox_messages(tenant_id);

DROP INDEX IF EXISTS idx_chat_phone_mappings_tenant_phone;

CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_phone_mappings_account_phone
    ON chat_phone_mappings(tenant_account_id, phone_normalized);

COMMIT;
