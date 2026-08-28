-- Admin notifications persist chat_id without a campaign_target.
-- Production still had NOT NULL from 000009, so UpsertAdminChatMapping silently failed.
ALTER TABLE chat_phone_mappings
    ALTER COLUMN campaign_id DROP NOT NULL,
    ALTER COLUMN campaign_target_id DROP NOT NULL;
