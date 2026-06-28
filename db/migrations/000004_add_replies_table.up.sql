-- Create replies table
CREATE TABLE IF NOT EXISTS campaign_replies (
	id UUID PRIMARY KEY DEFAULT uuidv7(),
	campaign_target_id UUID NOT NULL REFERENCES campaign_targets(id) ON DELETE CASCADE,
	message_text TEXT NOT NULL,
	received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_campaign_replies_target_id ON campaign_replies(campaign_target_id);
