-- Add deleted column and replace original_excel_data with original_excel_path
ALTER TABLE campaigns 
ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE campaigns 
ADD COLUMN IF NOT EXISTS original_excel_path TEXT;

ALTER TABLE campaigns 
ADD COLUMN IF NOT EXISTS processed_excel_path TEXT;

-- Copy existing data (if any)
-- Note: We can't really move the binary data to disk here, but we'll add the column
