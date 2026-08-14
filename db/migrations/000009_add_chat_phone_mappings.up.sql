CREATE TABLE chat_phone_mappings (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    chat_id TEXT NOT NULL,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    campaign_target_id UUID NOT NULL REFERENCES campaign_targets(id) ON DELETE CASCADE,
    phone_normalized VARCHAR(20) NOT NULL,
    viewer_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_chat_phone_mappings_chat_id ON chat_phone_mappings(chat_id);
CREATE UNIQUE INDEX ux_chat_phone_mappings_campaign_target_id ON chat_phone_mappings(campaign_target_id);
CREATE INDEX idx_chat_phone_mappings_phone ON chat_phone_mappings(phone_normalized);
