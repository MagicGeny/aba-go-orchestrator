ALTER TABLE campaigns
DROP COLUMN IF EXISTS start_immediately,
DROP COLUMN IF EXISTS time_to_start;
