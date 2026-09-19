-- Retry / attempt tracking on campaign targets
ALTER TABLE campaign_targets
    ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(64),
    ADD COLUMN IF NOT EXISTS last_error_message TEXT;

CREATE INDEX IF NOT EXISTS idx_campaign_targets_retry_pending
    ON campaign_targets (status, next_attempt_at)
    WHERE status = 'retry_pending';

-- Per-account daily usage (separate from tenant cold/warm quota)
CREATE TABLE IF NOT EXISTS tenant_account_daily_usage (
    tenant_account_id UUID NOT NULL REFERENCES tenant_accounts(id) ON DELETE CASCADE,
    usage_date DATE NOT NULL,
    used_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_account_id, usage_date)
    );