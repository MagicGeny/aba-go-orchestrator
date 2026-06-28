-- Add original_excel_data column to store the uploaded Excel file as bytes
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS original_excel_data BYTEA;
