-- Rollback migration
ALTER TABLE campaigns 
DROP COLUMN IF EXISTS deleted;

ALTER TABLE campaigns 
DROP COLUMN IF EXISTS original_excel_path;

ALTER TABLE campaigns 
DROP COLUMN IF EXISTS processed_excel_path;
