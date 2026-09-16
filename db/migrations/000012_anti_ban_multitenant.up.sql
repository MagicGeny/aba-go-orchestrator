-- Campaign scheduling metadata and VK fallback flag
ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS estimated_days INT,
    ADD COLUMN IF NOT EXISTS scheduled_completion_date TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fallback_to_vk_allowed BOOLEAN NOT NULL DEFAULT FALSE;

-- Multi-messenger support on targets
ALTER TABLE campaign_targets
    ADD COLUMN IF NOT EXISTS messenger_type VARCHAR(50) NOT NULL DEFAULT 'MAX';

-- New terminal status for cold phone lookup failures
ALTER TYPE task_status_enum ADD VALUE 'user_not_found_by_phone';

-- Tenant-scoped chat phone mappings with messenger dimension
ALTER TABLE chat_phone_mappings
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS messenger_type VARCHAR(50) NOT NULL DEFAULT 'MAX';

UPDATE chat_phone_mappings m
SET tenant_id = c.tenant_id
FROM campaigns c
WHERE m.campaign_id = c.id
  AND m.tenant_id IS NULL;

DELETE FROM chat_phone_mappings
WHERE tenant_id IS NULL;

ALTER TABLE chat_phone_mappings
    ALTER COLUMN tenant_id SET NOT NULL;

DELETE FROM chat_phone_mappings
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY tenant_id, phone_normalized, messenger_type
                   ORDER BY updated_at DESC, created_at DESC, id DESC
               ) AS rn
        FROM chat_phone_mappings
    ) ranked
    WHERE rn > 1
);

DROP INDEX IF EXISTS ux_chat_phone_mappings_chat_id;
CREATE UNIQUE INDEX IF NOT EXISTS ux_chat_phone_mappings_tenant_phone_messenger
    ON chat_phone_mappings (tenant_id, phone_normalized, messenger_type);
CREATE UNIQUE INDEX IF NOT EXISTS ux_chat_phone_mappings_chat_id ON chat_phone_mappings (chat_id);

CREATE INDEX IF NOT EXISTS idx_campaign_targets_pending
    ON campaign_targets (status)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_campaigns_processing_tenant
    ON campaigns (tenant_id)
    WHERE status = 'processing' AND deleted = FALSE;

-- Per-tenant daily send quotas (cold/warm counters isolated by tenant_id)
CREATE TABLE IF NOT EXISTS tenant_daily_quotas (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    quota_date DATE NOT NULL,
    cold_limit INT NOT NULL,
    cold_used INT NOT NULL DEFAULT 0,
    warm_used INT NOT NULL DEFAULT 0,
    last_cold_publish_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, quota_date)
);

CREATE INDEX IF NOT EXISTS idx_tenant_daily_quotas_date ON tenant_daily_quotas (quota_date);

-- Deferred publishing for outbox dosing
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS publish_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE;

UPDATE outbox_messages o
SET tenant_id = (o.payload->>'tenant_id')::uuid
WHERE o.tenant_id IS NULL
  AND o.payload ? 'tenant_id'
  AND (o.payload->>'tenant_id') ~* '^[0-9a-f-]{36}$';

CREATE INDEX IF NOT EXISTS idx_outbox_pending_publish
    ON outbox_messages (publish_at)
    WHERE status = 'pending';
