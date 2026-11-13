-- ============================================================
-- Drev CI Migration: 005_nullable_run_org_id.sql
-- Non-destructive, idempotent upgrade to allow runs without an organization.
-- Webhook-created runs and single-tenant runs legitimately have no organization mapping.
-- When org_id is provided, the foreign-key constraint to organizations(id) is preserved.
-- Safe to apply to fresh databases or already-migrated databases.
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA
      AND table_name = 'runs'
      AND column_name = 'org_id'
      AND is_nullable = 'NO'
  ) THEN
    ALTER TABLE runs ALTER COLUMN org_id DROP NOT NULL;
  END IF;
END $$;
