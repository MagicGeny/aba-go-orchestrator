DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'campaigns' AND column_name = 'attachment_path'
  ) THEN
    ALTER TABLE campaigns RENAME COLUMN attachment_path TO attachment_url;
  END IF;
END $$;
