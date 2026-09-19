DROP TABLE IF EXISTS tenant_account_daily_usage;

DROP INDEX IF EXISTS idx_campaign_targets_retry_pending;

ALTER TABLE campaign_targets
DROP COLUMN IF EXISTS last_error_message,
    DROP COLUMN IF EXISTS last_error_code,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS attempt_count;