-- ============================================================
-- Drev CI Migration: 006_run_diagnostics.sql
-- Idempotent schema upgrade for ghost-run and failure diagnostics.
-- Adds nullable failure_reason and interrupted_stage to runs,
-- and failure_reason to run_jobs.
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs' AND column_name = 'failure_reason'
  ) THEN
    ALTER TABLE runs ADD COLUMN failure_reason TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs' AND column_name = 'interrupted_stage'
  ) THEN
    ALTER TABLE runs ADD COLUMN interrupted_stage TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'run_jobs' AND column_name = 'failure_reason'
  ) THEN
    ALTER TABLE run_jobs ADD COLUMN failure_reason TEXT;
  END IF;
END $$;
