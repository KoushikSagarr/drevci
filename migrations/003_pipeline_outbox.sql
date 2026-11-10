-- ============================================================
-- Drev CI: Pipeline Outbox Schema (Minimal Durable Outbox)
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_outbox (
  run_id        UUID PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
  pipeline_spec TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_outbox_created_at ON pipeline_outbox(created_at ASC);
