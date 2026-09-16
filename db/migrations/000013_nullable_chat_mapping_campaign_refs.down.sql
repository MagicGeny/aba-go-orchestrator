DELETE FROM chat_phone_mappings
WHERE campaign_id IS NULL OR campaign_target_id IS NULL;

ALTER TABLE chat_phone_mappings
    ALTER COLUMN campaign_id SET NOT NULL,
    ALTER COLUMN campaign_target_id SET NOT NULL;
