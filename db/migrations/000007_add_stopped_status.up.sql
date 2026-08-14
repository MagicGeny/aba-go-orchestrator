-- PostgreSQL requires ALTER TYPE ... ADD VALUE to run outside a transaction.
-- This migration adds the 'stopped' value to the campaign_status_enum.
ALTER TYPE campaign_status_enum ADD VALUE 'stopped';
