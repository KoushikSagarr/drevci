-- ============================================================
-- Drev CI Migration: 004_fix_uuid_types.sql
-- Non-destructive, idempotent upgrade for existing installations
-- that may have applied earlier versions of webhook_deliveries
-- or pipeline_outbox with run_id TEXT, or missing foreign keys.
--
-- Safely converts run_id from TEXT to UUID using in-place casting,
-- preserves all existing records, and adds foreign key constraints.
-- Safe to apply to fresh databases or already-migrated databases.
-- ============================================================

DO $$
BEGIN
  -- 1. webhook_deliveries
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'webhook_deliveries'
  ) THEN
    -- 1a. Convert run_id to UUID if currently text
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = CURRENT_SCHEMA
        AND table_name = 'webhook_deliveries'
        AND column_name = 'run_id'
        AND data_type = 'text'
    ) THEN
      ALTER TABLE webhook_deliveries
        ALTER COLUMN run_id TYPE UUID USING run_id::uuid;
    END IF;

    -- 1b. Add foreign key if missing and runs table exists
    IF EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs'
    ) AND NOT EXISTS (
      SELECT 1 FROM information_schema.table_constraints
      WHERE table_schema = CURRENT_SCHEMA
        AND table_name = 'webhook_deliveries'
        AND constraint_type = 'FOREIGN KEY'
    ) THEN
      ALTER TABLE webhook_deliveries
        ADD CONSTRAINT fk_webhook_deliveries_run_id
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;
    END IF;
  END IF;

  -- 2. pipeline_outbox
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'pipeline_outbox'
  ) THEN
    -- 2a. Convert run_id to UUID if currently text
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = CURRENT_SCHEMA
        AND table_name = 'pipeline_outbox'
        AND column_name = 'run_id'
        AND data_type = 'text'
    ) THEN
      ALTER TABLE pipeline_outbox
        ALTER COLUMN run_id TYPE UUID USING run_id::uuid;
    END IF;

    -- 2b. Add foreign key if missing and runs table exists
    IF EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs'
    ) AND NOT EXISTS (
      SELECT 1 FROM information_schema.table_constraints
      WHERE table_schema = CURRENT_SCHEMA
        AND table_name = 'pipeline_outbox'
        AND constraint_type = 'FOREIGN KEY'
    ) THEN
      ALTER TABLE pipeline_outbox
        ADD CONSTRAINT fk_pipeline_outbox_run_id
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;
    END IF;
  END IF;
END $$;
