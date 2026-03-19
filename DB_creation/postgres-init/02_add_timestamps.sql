-- Add created_at to existing sandbox tables (run once if tables already exist without timestamps)
-- Safe to run: uses IF NOT EXISTS / DO blocks so it won't fail if columns already exist.
\connect ava_sandbox

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'main_buyers' AND column_name = 'created_at'
  ) THEN
    ALTER TABLE main_buyers ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'main_leads' AND column_name = 'created_at'
  ) THEN
    ALTER TABLE main_leads ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'main_cars' AND column_name = 'created_at'
  ) THEN
    ALTER TABLE main_cars ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'main_pickups' AND column_name = 'created_at'
  ) THEN
    ALTER TABLE main_pickups ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_main_leads_created_at ON main_leads (created_at);
CREATE INDEX IF NOT EXISTS idx_main_cars_created_at ON main_cars (created_at);
CREATE INDEX IF NOT EXISTS idx_main_pickups_created_at ON main_pickups (created_at);
