-- ============================================================
-- Drev CI Migration: 007_cancellation_intent.sql
-- Idempotent schema upgrade for durable run cancellation intent.
-- Adds nullable cancellation_requested_at to runs.
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA AND table_name = 'runs' AND column_name = 'cancellation_requested_at'
  ) THEN
    ALTER TABLE runs ADD COLUMN cancellation_requested_at TIMESTAMPTZ;
  END IF;
END $$;
