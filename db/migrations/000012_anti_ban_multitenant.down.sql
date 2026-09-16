DROP INDEX IF EXISTS idx_outbox_pending_publish;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS publish_at;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS tenant_id;

DROP TABLE IF EXISTS tenant_daily_quotas;

DROP INDEX IF EXISTS ux_chat_phone_mappings_tenant_phone_messenger;
CREATE UNIQUE INDEX IF NOT EXISTS ux_chat_phone_mappings_chat_id ON chat_phone_mappings (chat_id);

ALTER TABLE chat_phone_mappings DROP COLUMN IF EXISTS messenger_type;
ALTER TABLE chat_phone_mappings DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE campaign_targets DROP COLUMN IF EXISTS messenger_type;

ALTER TABLE campaigns DROP COLUMN IF EXISTS fallback_to_vk_allowed;
ALTER TABLE campaigns DROP COLUMN IF EXISTS scheduled_completion_date;
ALTER TABLE campaigns DROP COLUMN IF EXISTS estimated_days;
