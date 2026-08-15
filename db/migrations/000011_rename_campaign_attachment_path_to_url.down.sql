DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'campaigns' AND column_name = 'attachment_url'
  ) THEN
    ALTER TABLE campaigns RENAME COLUMN attachment_url TO attachment_path;
  END IF;
END $$;
