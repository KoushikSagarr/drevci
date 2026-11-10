-- ============================================================
-- Drev CI: Webhook Deliveries Schema (Persistent Idempotency)
-- ============================================================

CREATE TABLE IF NOT EXISTS webhook_deliveries (
  delivery_id TEXT PRIMARY KEY,
  event_type  TEXT NOT NULL,
  repo        TEXT NOT NULL DEFAULT '',
  pr_number   INTEGER NOT NULL DEFAULT 0,
  run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_run_id ON webhook_deliveries(run_id);
