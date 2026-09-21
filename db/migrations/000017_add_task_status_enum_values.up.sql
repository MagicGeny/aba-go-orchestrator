ALTER TYPE task_status_enum ADD VALUE IF NOT EXISTS 'retry_pending';
ALTER TYPE task_status_enum ADD VALUE IF NOT EXISTS 'delivery_unknown';